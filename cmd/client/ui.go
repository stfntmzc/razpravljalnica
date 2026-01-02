package main

import (
	"fmt"
	"os"
	"razpravljalnica/client"
	"sort"
	"strings"

	"github.com/mattn/go-runewidth"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// konstante
const (
	appTitle = "Razpravljalnica"

	marginLeft = 2
	marginTop  = 1

	contentWidth             = 100
	contentHeight            = 30
	contnetPadddingSides     = 3
	contnetPadddingTopBottom = 1

	maxTopicNameLength = 59

	tabsPadding = 1

	footerHeight = 1

	topLeftChar        = "╭"
	topRightChar       = "╮"
	bottomLeftChar     = "╰"
	bottomRightChar    = "╯"
	horizontalLineChar = "─"
	verticalLineChar   = "│"
	TrightChar         = "├"
	TleftChar          = "┤"
	TdownChar          = "┬"
	TupChar            = "┴"
	crossChar          = "┼"

	// message item stvari
	messageItemWidth        = 71
	messageItemMarginBottom = 1

	newMessageInputHeight = 2

	cursorChar = ">"
)

type model struct {
	quitting bool
	loggedIn bool
	// stvari za projavo
	/*username string
	urlHead  string
	urlTail  string*/
	inputs []textinput.Model
	focus  int

	tabs                []string
	openTabIndex        int
	cursorIndexes       map[int]int // key je index od taba
	contentStartIndexes map[int]int // key je index od taba
	contentEndIndexes   map[int]int // key je index od taba

	// topics
	createTopicMode  bool
	createTopicInput textinput.Model
	topics           map[int64]string
	// messages
	openTopicId         int64
	messages            map[int64]client.UiMessageItem
	cursorMessagesIndex int
	selectedMessageId   int
	messagesStartIndex  int
	messagesEndIndex    int
	postNewMessageMode  bool
	postNewMessageInput textinput.Model
	editMessageMode     bool

	// client
	client connectResultMsg
}

type messageItem struct {
	username  string
	id        int64
	timestamp *timestamppb.Timestamp
	likes     int64
	text      []string // array stringov širine messageItemWidth
}

type connectResultMsg struct {
	clientState *client.ClientState
	err         error
}

type listTopicsMsg struct {
	topics map[int64]string
	err    error
}

type listMessagesMsg struct {
	messages map[int64]client.UiMessageItem
	err      error
}

type createTopicMsg struct {
	id  int64
	err error
}

type likeMessageMsg struct {
	messageId int
	err       error
}

type deleteMessageMsg struct {
	messageId int
	err       error
}

type postNewMessageMsg struct {
	message *client.UiMessageItem
	err     error
}

func initialModel() model {
	inputs := make([]textinput.Model, 3)

	// login text input
	inputs[0] = textinput.New()
	inputs[0].Placeholder = "Username"
	inputs[0].Focus()
	inputs[0].CharLimit = 32

	inputs[1] = textinput.New()
	inputs[1].Placeholder = "Head node URL"

	inputs[2] = textinput.New()
	inputs[2].Placeholder = "Tail node URL"

	inputs[0].Prompt = ""
	inputs[1].Prompt = ""
	inputs[2].Prompt = ""

	// create topic text input
	createTopicInput := textinput.New()
	createTopicInput.Placeholder = "Topic name"
	createTopicInput.CharLimit = maxTopicNameLength
	createTopicInput.Prompt = ""

	// post message text input
	postNewMessageInput := textinput.New()
	postNewMessageInput.Placeholder = "New message"
	postNewMessageInput.CharLimit = messageItemWidth * (newMessageInputHeight + 1)
	postNewMessageInput.Prompt = ""

	// tabs in indexi
	tabs := []string{"Topics", "Live chat"}
	cursorIndexes := make(map[int]int)
	contentStartIndexes := make(map[int]int)
	contentEndIndexes := make(map[int]int)
	for i := 0; i < len(tabs); i++ {
		cursorIndexes[i] = 0
		contentStartIndexes[i] = 0
		contentEndIndexes[i] = 0
	}

	// odprt topic
	openTopicId := -1
	messages := make(map[int64]client.UiMessageItem)

	return model{
		inputs:              inputs,
		tabs:                tabs,
		openTabIndex:        0,
		topics:              make(map[int64]string),
		cursorIndexes:       cursorIndexes,
		contentStartIndexes: contentStartIndexes,
		contentEndIndexes:   contentEndIndexes,
		createTopicInput:    createTopicInput,
		createTopicMode:     false,
		openTopicId:         int64(openTopicId),
		messages:            messages,
		postNewMessageMode:  false,
		postNewMessageInput: postNewMessageInput,
		editMessageMode:     false,
	}
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if !m.loggedIn {
		switch msg := msg.(type) {

		case tea.KeyMsg:
			switch msg.String() {

			case "ctrl+c", "q":
				m.quitting = true
				return m, tea.Quit

			case "down":
				if !m.loggedIn {
					m.focus = (m.focus + 1) % len(m.inputs)
					m.updateFocus()
				}

			case "up":
				if !m.loggedIn {
					m.focus--
					if m.focus < 0 {
						m.focus = len(m.inputs) - 1
					}
					m.updateFocus()
				}

			case "enter":
				if !m.loggedIn {
					if m.focus < len(m.inputs)-1 {
						// premik dol
						m.focus++
						m.updateFocus()
					} else {
						// grpc connect
						return m, connectCmd(m.inputs[0].Value(), m.inputs[1].Value(), m.inputs[2].Value())
					}
				}
			}
		case connectResultMsg:
			if msg.err != nil {
				m.client.err = msg.err
				m.loggedIn = false
				return m, nil
			}
			// povezava vspostavljena
			m.client.clientState = msg.clientState
			m.loggedIn = true
			return m, listTopicsCmd(m)
		}
		var cmd tea.Cmd
		m.inputs[m.focus], cmd = m.inputs[m.focus].Update(msg)
		return m, cmd

	} else {
		// smo prijavljeni
		switch msg := msg.(type) {
		case tea.KeyMsg:
			// delamo nov topic
			if m.createTopicMode {
				switch msg.String() {
				case "esc":
					m.createTopicMode = false
					m.createTopicInput.SetValue("")
					m.createTopicInput.Blur()
					return m, nil

				case "enter":
					name := m.createTopicInput.Value()
					if name != "" {
						m.createTopicMode = false
						m.createTopicInput.SetValue("")
						m.createTopicInput.Blur()
						return m, createTopicCmd(m, name)
					}
					return m, nil
				}

				var cmd tea.Cmd
				m.createTopicInput, cmd = m.createTopicInput.Update(msg)
				return m, cmd
			} else if m.postNewMessageMode {
				// postamo nov message
				switch msg.String() {
				case "esc":
					m.postNewMessageMode = false
					m.postNewMessageInput.SetValue("")
					m.postNewMessageInput.Blur()
					m.messagesStartIndex = max(0, m.messagesStartIndex-newMessageInputHeight)
					return m, nil
				case "enter":
					text := m.postNewMessageInput.Value()

					wrapped := wrapText(text, messageItemWidth)
					// damo stran vrstice, ki jih je preveč
					if len(wrapped) > newMessageInputHeight+1 {
						wrapped = wrapped[:newMessageInputHeight+1]
					}
					// združimo nazaj
					limitedText := strings.Join(wrapped, " ")
					if limitedText != "" {
						m.postNewMessageMode = false
						m.postNewMessageInput.SetValue("")
						m.postNewMessageInput.Blur()
						m.messagesStartIndex = max(0, m.messagesStartIndex-newMessageInputHeight)
						return m, postNewMessageCmd(m, limitedText)
					}
					return m, nil
				}
				var cmd tea.Cmd
				m.postNewMessageInput, cmd = m.postNewMessageInput.Update(msg)
				return m, cmd
			} else if m.editMessageMode {
				// editamp message
				switch msg.String() {
				case "esc":
					m.editMessageMode = false
					m.postNewMessageInput.SetValue("")
					m.postNewMessageInput.Blur()
					m.messagesStartIndex = max(0, m.messagesStartIndex-newMessageInputHeight)
					return m, nil
				case "enter":
					text := m.postNewMessageInput.Value()

					wrapped := wrapText(text, messageItemWidth)
					// damo stran vrstice, ki jih je preveč
					if len(wrapped) > newMessageInputHeight+1 {
						wrapped = wrapped[:newMessageInputHeight+1]
					}
					// združimo nazaj
					limitedText := strings.Join(wrapped, " ")
					if limitedText != "" {
						m.editMessageMode = false
						m.postNewMessageInput.SetValue("")
						m.postNewMessageInput.Blur()
						m.messagesStartIndex = max(0, m.messagesStartIndex-newMessageInputHeight)
						messageId := getSelectedMessageId(m)
						return m, editMessageCmd(m, messageId, limitedText)
					}
					return m, nil
				}
				var cmd tea.Cmd
				m.postNewMessageInput, cmd = m.postNewMessageInput.Update(msg)
				return m, cmd
			} else if m.openTopicId != -1 {
				// beremo sporočila nekega topica
				switch msg.String() {
				case "b":
					m.openTopicId = -1
				case "right":
					if m.openTabIndex < len(m.tabs)-1 {
						m.openTabIndex++
					}
				case "left":
					if m.openTabIndex > 0 {
						m.openTabIndex--
					}
				case "up":
					if m.cursorMessagesIndex > 0 {
						m.cursorMessagesIndex--
						// prišli smo do roba
						if m.cursorMessagesIndex < m.messagesStartIndex {
							m.messagesStartIndex--
							m.messagesEndIndex--
						}
					}
				case "down":
					contentLen := getLastMessageCursorIndex(m) + 1

					if m.cursorMessagesIndex < contentLen-1 {
						m.cursorMessagesIndex++

						if m.cursorMessagesIndex > m.messagesEndIndex {
							m.messagesStartIndex++
							m.messagesEndIndex++
						}

						if m.messagesEndIndex >= contentLen {
							m.messagesEndIndex = contentLen - 1
							viewportHeight := contentHeight - 2*contnetPadddingTopBottom - 2
							m.messagesStartIndex = max(0, m.messagesEndIndex-viewportHeight)
						}
					}
				case "q":
					m.quitting = true
					return m, tea.Quit
				case "l":
					messageId := getSelectedMessageId(m)
					if m.client.clientState.User.Id == m.messages[int64(messageId)].UserId {
						// ne moreš likeat sam svoj message
						return m, nil
					}
					return m, likeMessageCmd(m, messageId)
				case "e":
					messageId := getSelectedMessageId(m)
					if m.client.clientState.User.Id != m.messages[int64(messageId)].UserId {
						// ne moreš editat message ki ni tvoj
						m.editMessageMode = false
						return m, nil
					}
					m.messagesStartIndex += newMessageInputHeight
					// uporabmo isti input kt za nov message
					m.postNewMessageInput.Focus()
					m.editMessageMode = true
					//return m, editMessageCmd(m, messageId, m.postNewMessageInput.Value())
					return m, nil
				case "d":
					messageId := getSelectedMessageId(m)
					if m.client.clientState.User.Id != m.messages[int64(messageId)].UserId {
						// ne moreš brisat tujih sporočil
						return m, nil
					}
					return m, deleteMessageCmd(m, messageId)
				case "p":
					// postamo nov message
					m.messagesStartIndex += newMessageInputHeight
					m.postNewMessageMode = true
					m.postNewMessageInput.Focus()
					return m, nil
				}
			} else {
				switch msg.String() {
				case "ctrl+c", "q":
					m.quitting = true
					return m, tea.Quit
				case "c":
					// delamo nov topic
					if m.tabs[m.openTabIndex] == "Topics" {
						m.createTopicMode = true
						m.createTopicInput.Focus()
						return m, nil
					}
				case "right":
					if m.openTabIndex < len(m.tabs)-1 {
						m.openTabIndex++
					}
				case "left":
					if m.openTabIndex > 0 {
						m.openTabIndex--
					}
				case "up":
					if m.tabs[m.openTabIndex] == "Topics" {
						/*if m.cursorIndexes[m.openTabIndex] > 0 {
							m.cursorIndexes[m.openTabIndex]--
						}
						if m.cursorIndexes[m.openTabIndex] > 0 {
							m.cursorIndexes[m.openTabIndex]--
						} else if m.contentEndIndexes[m.openTabIndex] < len(m.topics)-1 {
							m.contentStartIndexes[m.openTabIndex]++
							m.contentEndIndexes[m.openTabIndex]++
							m.cursorIndexes[m.openTabIndex]++
						}*/
						if m.cursorIndexes[m.openTabIndex] > 0 {
							if m.cursorIndexes[m.openTabIndex] <= m.contentStartIndexes[m.openTabIndex] {
								m.contentStartIndexes[m.openTabIndex]--
								m.contentEndIndexes[m.openTabIndex]--
								m.cursorIndexes[m.openTabIndex]--
							} else {
								m.cursorIndexes[m.openTabIndex]--
							}
						}

					} else if m.tabs[m.openTabIndex] == "Live chat" {
						// TODO
					}
				case "down":
					if m.tabs[m.openTabIndex] == "Topics" {
						/*if m.cursorIndexes[m.openTabIndex] < 27 {
							m.cursorIndexes[m.openTabIndex]++
						}*/
						if m.cursorIndexes[m.openTabIndex] < m.contentEndIndexes[m.openTabIndex] {
							m.cursorIndexes[m.openTabIndex]++
						} else if m.contentEndIndexes[m.openTabIndex] < len(m.topics)-1 {
							m.contentStartIndexes[m.openTabIndex]++
							m.contentEndIndexes[m.openTabIndex]++
							m.cursorIndexes[m.openTabIndex]++
						}
					} else if m.tabs[m.openTabIndex] == "Live chat" {
						// TODO
					}
				case "m":
					if m.tabs[m.openTabIndex] == "Topics" && m.cursorIndexes[m.openTabIndex] >= 0 {
						ids := make([]int64, 0, len(m.topics))
						for id := range m.topics {
							ids = append(ids, id)
						}
						sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

						topicId := m.cursorIndexes[0]
						m.openTopicId = int64(ids[topicId])

						m.openTopicId = int64(ids[topicId])
						m.messages = make(map[int64]client.UiMessageItem) // reset
						return m, listMessagesCmd(m)
					}
				}
			}
		case listTopicsMsg:
			if msg.err != nil {
				return m, nil
			}

			oldLen := len(m.topics)
			newLen := len(msg.topics)
			viewport := contentHeight - 2*contnetPadddingTopBottom - 1

			m.topics = msg.topics

			// če je vsebine manj kot viewport
			if newLen <= viewport {
				m.contentStartIndexes[0] = 0
				m.contentEndIndexes[0] = newLen - 1
				m.cursorIndexes[0] = min(m.cursorIndexes[0], newLen-1)
				return m, nil
			}

			// če smo bili na dnu, ostanemo na dnu
			if m.contentEndIndexes[0] == oldLen-1 {
				m.contentEndIndexes[0] = newLen - 1
				m.contentStartIndexes[0] = newLen - viewport
				m.cursorIndexes[0] = newLen - 1
			} else {
				// sicer samo razširimo konec
				m.contentEndIndexes[0] = min(
					m.contentStartIndexes[0]+viewport,
					newLen-1,
				)
			}

			return m, nil
		case createTopicMsg:
			// naredu se je nov topic
			if msg.err == nil {
				// ponovno listamo topice
				return m, listTopicsCmd(m)
			}
			return m, nil
		case listMessagesMsg:
			if msg.err != nil {
				return m, nil
			}
			m.messages = msg.messages
			m.cursorMessagesIndex = getLastMessageCursorIndex(m)
			// da vemo od kje do kje pokazati messages
			viewport := contentHeight - 2*contnetPadddingTopBottom - 2
			m.messagesEndIndex = m.cursorMessagesIndex
			m.messagesStartIndex = max(0, m.messagesEndIndex-viewport+1)

			return m, nil
		case likeMessageMsg:
			if msg.err != nil {
				return m, nil
			}
			message := m.messages[int64(msg.messageId)]
			message.Likes++
			m.messages[int64(msg.messageId)] = message
			return m, nil
		case postNewMessageMsg:
			if msg.err != nil {
				return m, nil
			}
			//m.messages[msg.message.Id] = *msg.message
			return m, listMessagesCmd(m)
		case deleteMessageMsg:
			if msg.err != nil {
				return m, nil
			}
			delete(m.messages, int64(msg.messageId))
			// popravimo cursor, da ne kaže izven
			m.cursorMessagesIndex = max(0, m.cursorMessagesIndex-1)
			// ponovno listamo messages
			return m, listMessagesCmd(m)
		}
		var cmd tea.Cmd
		m.inputs[m.focus], cmd = m.inputs[m.focus].Update(msg)
		return m, cmd
	}

	//return m, nil
}

func (m *model) updateFocus() {
	for i := range m.inputs {
		m.inputs[i].Blur()
	}
	m.inputs[m.focus].Focus()
}

func (m model) View() string {
	if m.quitting {
		return "\n	See you later!\n\n"
	}
	if !m.loggedIn {
		return loginView(m)
	}
	return homeView(m)
}

func homeView(m model) string {
	s := ""
	// margin
	for i := 0; i < marginTop; i++ {
		s += "\n"
	}
	/*for i := 0; i < marginLeft; i++ {
		s += " "
	}*/
	s += getFillWithString(m, marginLeft, " ")

	// zgornji rob in tabs
	s += getTabsString(m) + "\n"
	s += getContentString(m)

	return s
}

func loginView(m model) string {
	s := "Razpravljalnica\nLogin\n\n"

	labels := []string{
		"Username	",
		"Head node url	",
		"Tail node url	",
	}

	for i, input := range m.inputs {
		cursor := " "
		if m.focus == i {
			cursor = ">"
		}
		s += fmt.Sprintf("	%s %s %s\n", cursor, labels[i], input.View())
	}

	return s
}

func getTabsString(m model) string {
	s := ""
	var totalTbasWidth = 0
	for i := 0; i < len(m.tabs); i++ {
		s += topLeftChar
		totalTbasWidth++
		for j := 0; j < len(m.tabs[i])+2*tabsPadding; j++ {
			s += horizontalLineChar
			totalTbasWidth++
		}
		s += topRightChar
		totalTbasWidth++
	}
	s += "\n"
	for i := 0; i < marginLeft; i++ {
		s += " "
	}
	for i := 0; i < len(m.tabs); i++ {
		s += verticalLineChar
		for j := 0; j < tabsPadding; j++ {
			s += " "
		}
		s += m.tabs[i]
		for j := 0; j < tabsPadding; j++ {
			s += " "
		}
		s += verticalLineChar
	}
	s += "\n"
	for i := 0; i < marginLeft; i++ {
		s += " "
	}
	for i := 0; i < len(m.tabs); i++ {
		if i == m.openTabIndex {
			if i == 0 {
				s += verticalLineChar
			} else {
				s += bottomRightChar
			}
		} else {
			if i == 0 {
				s += TrightChar
			} else {
				s += TupChar
			}
		}
		for j := 0; j < len(m.tabs[i])+2*tabsPadding; j++ {
			if i == m.openTabIndex {
				s += " "
			} else {
				s += horizontalLineChar
			}
		}
		if i == m.openTabIndex {
			s += bottomLeftChar
		} else {
			s += TupChar
		}
	}
	s += getFillWithString(m, contentWidth+1-totalTbasWidth, horizontalLineChar) + topRightChar
	return s
}

func gatMarginLeftString(m model) string {
	s := ""
	for i := 0; i < marginLeft; i++ {
		s += " "
	}
	return s
}

func getContnetPaddingSidesString(m model) string {
	s := ""
	for i := 0; i < contnetPadddingSides; i++ {
		s += " "
	}
	return s
}

func getFillWithString(m model, len int, c string) string {
	s := ""
	for i := 0; i < len; i++ {
		s += c
	}
	return s
}

func getContnetPaddingTopBottomString(m model) string {
	s := ""
	for i := 0; i < contnetPadddingTopBottom; i++ {
		s += gatMarginLeftString(m) + verticalLineChar + getFillWithString(m, contentWidth, " ") + verticalLineChar + "\n"
	}
	return s
}

func getContentString(m model) string {
	s := ""

	// zgornji padding
	s += gatMarginLeftString(m) + verticalLineChar + getFillWithString(m, contentWidth, " ") + verticalLineChar + "\n"

	// topics -----------------------------
	if m.tabs[m.openTabIndex] == "Topics" {
		if m.openTopicId != -1 {
			s += getOpenTopicString(m)
		} else {
			s += getTopicsString(m)
		}
	} else if m.tabs[m.openTabIndex] == "Live chat" {
		s += getLiveChatView(m)
	}

	return s
}

func getTabsPadding(m model) string {
	return getFillWithString(m, tabsPadding, " ")
}

func getLiveChatView(m model) string {
	s := ""

	if len(m.client.clientState.Subscriptions) == 0 {
		noSubscriptionstext := "You are not subscribed to any topics."
		s += gatMarginLeftString(m) + verticalLineChar + getContnetPaddingSidesString(m) + noSubscriptionstext
		s += getFillWithString(m, contentWidth-(runewidth.StringWidth(noSubscriptionstext)+contnetPadddingSides), " ")
		s += verticalLineChar + "\n"
		for i := contnetPadddingTopBottom * 2; i < contentHeight; i++ {
			s += gatMarginLeftString(m) + verticalLineChar + getFillWithString(m, contentWidth, " ") + verticalLineChar + "\n"
		}
		// footer
		s += gatMarginLeftString(m) + TrightChar + getFillWithString(m, contentWidth, horizontalLineChar) + TleftChar + "\n"
		s += gatMarginLeftString(m) + verticalLineChar + getTabsPadding(m)
		legendString := "q - quit"
		s += legendString + getFillWithString(m, contentWidth-(len(legendString)+tabsPadding), " ") + verticalLineChar + "\n"
		s += gatMarginLeftString(m) + bottomLeftChar + getFillWithString(m, contentWidth, horizontalLineChar) + bottomRightChar + "\n"
	}

	return s
}

func getTopicsString(m model) string {
	s := ""

	// treba je sortirat po id-jih
	ids := make([]int64, 0, len(m.topics))
	for id := range m.topics {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// content
	iStart := m.contentStartIndexes[0]
	//iEnd := contentHeight - contnetPadddingTopBottom*2 - 1
	//iEnd := m.contentEndIndexes[0]
	iEnd := max(contentHeight-contnetPadddingTopBottom*2-1, m.contentEndIndexes[0])
	for i := iStart; i <= iEnd; i++ {
		var contentIndex int
		s += gatMarginLeftString(m) + verticalLineChar
		if i < len(ids) {
			// ali je cursor na topicu
			if m.cursorIndexes[0] == i && !m.createTopicMode {
				s += " " + cursorChar + "  "
				name := m.topics[ids[i]]
				s += fmt.Sprintf("%s [%d]", name, ids[i])
				legendString := "m - messages " + verticalLineChar + " s - subscribe"
				s += getFillWithString(m, contentWidth-(4+len(name)+digits(ids[i])+3+len(legendString)+contnetPadddingSides-2), " ")
				s += legendString + getFillWithString(m, contnetPadddingSides, " ")
				//contentIndex = len(name) + digits(ids[i]) + 3 + 4
			} else {
				s += getContnetPaddingSidesString(m)
				name := m.topics[ids[i]]
				s += fmt.Sprintf("%s [%d]", name, ids[i])
				contentIndex = len(name) + digits(ids[i]) + 3 + contnetPadddingSides
				s += getFillWithString(m, contentWidth-contentIndex, " ")
			}
			s += verticalLineChar + "\n"
		} else {
			s += getFillWithString(m, contentWidth, " ") + verticalLineChar + "\n"
		}
	}
	s += getContnetPaddingTopBottomString(m)

	// footer
	s += gatMarginLeftString(m) + TrightChar + getFillWithString(m, contentWidth, horizontalLineChar) + TleftChar + "\n"
	s += gatMarginLeftString(m) + verticalLineChar + getTabsPadding(m)
	if m.createTopicMode {
		// delamo nov topic
		line := "New topic: " + m.createTopicInput.View()
		s += line + getFillWithString(m, contentWidth-(len(line)+tabsPadding)+8, " ") + verticalLineChar + "\n"
	} else {
		legendString := "c - create new topic " + verticalLineChar + " q - quit"
		s += legendString + getFillWithString(m, contentWidth-(len(legendString)+tabsPadding-2), " ") + verticalLineChar + "\n"
	}
	s += gatMarginLeftString(m) + bottomLeftChar + getFillWithString(m, contentWidth, horizontalLineChar) + bottomRightChar + "\n"

	return s
}

func getOpenTopicString(m model) string {
	s := ""

	currLineIndex := contnetPadddingTopBottom
	// ime topica
	legendString := "s - subscribe"
	s += getFillWithString(m, marginLeft, " ") + verticalLineChar + getContnetPaddingSidesString(m)
	s += m.topics[m.openTopicId] + fmt.Sprintf(" [%d]", m.openTopicId)
	s += getFillWithString(m, contentWidth-(len(m.topics[m.openTopicId])+digits(m.openTopicId)+3+len(legendString)+2*contnetPadddingSides), " ")
	s += legendString + getContnetPaddingSidesString(m) + verticalLineChar + "\n"
	currLineIndex++
	for i := 0; i < messageItemMarginBottom; i++ {
		s += getFillWithString(m, marginLeft, " ") + verticalLineChar + getFillWithString(m, contentWidth, " ") + verticalLineChar + "\n"
		currLineIndex++
	}

	// messages
	s += getMessageItemsString(m, currLineIndex)

	s += getContnetPaddingTopBottomString(m)
	// footer
	s += gatMarginLeftString(m) + TrightChar + getFillWithString(m, contentWidth, horizontalLineChar) + TleftChar + "\n"

	if m.postNewMessageMode || m.editMessageMode {
		// delamo nov message ali pa editamo nek message
		wrappedInput := wrapText(m.postNewMessageInput.Value(), messageItemWidth)
		if len(wrappedInput) == 0 {
			wrappedInput = []string{""}
		}
		prefix := "New message: "
		if m.editMessageMode {
			prefix = "Edit message: "
		}
		for i := 0; i < newMessageInputHeight+1; i++ {
			s += gatMarginLeftString(m) + verticalLineChar + getTabsPadding(m)
			var line string
			if i < len(wrappedInput) {
				line = wrappedInput[i]
			}
			if i == 0 {
				s += prefix + line
			} else {
				s += getFillWithString(m, len(prefix), " ") + line
			}
			s += getFillWithString(m, contentWidth-(len(prefix)+runewidth.StringWidth(line)+tabsPadding), " ")
			s += verticalLineChar + "\n"
		}

	} else {
		s += gatMarginLeftString(m) + verticalLineChar + getTabsPadding(m)
		legendString := "p - post new message " + verticalLineChar + " b - back " + verticalLineChar + " q - quit"
		s += legendString + getFillWithString(m, contentWidth-(len(legendString)+tabsPadding-4), " ") + verticalLineChar + "\n"
	}
	s += gatMarginLeftString(m) + bottomLeftChar + getFillWithString(m, contentWidth, horizontalLineChar) + bottomRightChar + "\n"

	return s
}

func getMessageItemsString(m model, currLineIndex int) string {
	s := ""

	messages := make([]client.UiMessageItem, 0, len(m.messages))
	for _, message := range m.messages {
		messages = append(messages, message)
	}

	// sortiramo po timestamp
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.AsTime().Before(messages[j].Timestamp.AsTime())
	})

	content, messageIds, printLegend := buildContentMessages(m, messages)

	//if m.cursorMessagesIndex == iEnd - contentHeight - 2*contnetPadddingTopBottom + 7
	iStart := m.messagesStartIndex
	iEnd := min(m.messagesEndIndex, len(content)-1)

	renderedLines := 0
	for i := iStart; i <= iEnd; i++ {
		line := gatMarginLeftString(m) + verticalLineChar
		if m.cursorMessagesIndex == i && !m.postNewMessageMode {
			line += " > "
		} else {
			line += getContnetPaddingSidesString(m)
		}
		line += content[i]
		if printLegend[i] && messageIds[m.cursorMessagesIndex] == messageIds[i] && !m.postNewMessageMode {
			//printLegend = false
			legendString := "l - like"
			if m.messages[int64(messageIds[i])].UserId == m.client.clientState.User.Id {
				legendString = "e - edit " + verticalLineChar + " d - delete"
			}
			line += getFillWithString(m, contentWidth-(runewidth.StringWidth(legendString)+messageItemWidth+2*contnetPadddingSides), " ")
			line += legendString
		} else {
			filler := getFillWithString(m, contentWidth-(runewidth.StringWidth(content[i])+2*contnetPadddingSides), " ")
			line += filler
		}
		line += getContnetPaddingSidesString(m) + verticalLineChar
		line += fmt.Sprintf("%d %d %d", messageIds[i], messageIds[m.cursorMessagesIndex], m.cursorMessagesIndex)
		s += line + "\n"
		renderedLines++
		/*if prevMessageId != messageIds[i] {
			printLegend = true
		}
		prevMessageId = messageIds[i]*/
	}
	m.selectedMessageId = messageIds[m.cursorMessagesIndex]

	// zapolnemo stran če je prazna
	viewport := contentHeight - 2*contnetPadddingTopBottom - 2
	if m.postNewMessageMode {
		viewport -= newMessageInputHeight
	}
	for renderedLines < viewport {
		emptyLine :=
			gatMarginLeftString(m) +
				verticalLineChar +
				getFillWithString(m, contentWidth, " ") +
				verticalLineChar

		s += emptyLine + "\n"
		renderedLines++
	}

	return s
}

// pomozna
// key: message ID; value: vrstica, kjer se text tega sporočila začne
func buildContentMessages(m model, messages []client.UiMessageItem) ([]string, map[int]int, map[int]bool) {
	var content []string
	ids := make(map[int]int)
	legend := make(map[int]bool)

	index := 0
	for i, message := range messages {
		// ime in likes
		nameString := fmt.Sprintf("%s [%d]", message.Username, message.Id)
		likesString := fmt.Sprintf("%d likes", message.Likes)
		//nameAndLikesString := nameString + getFillWithString(m, messageItemWidth-(len(nameString)+len(likesString)), " ") + likesString
		nameAndLikesString := nameString + getFillWithString(m, messageItemWidth-(runewidth.StringWidth(nameString)+runewidth.StringWidth(likesString)), " ") + likesString

		ids[index] = int(message.Id)
		legend[index] = false
		content = append(content, nameAndLikesString)
		index++
		// besedilo
		for j, line := range message.Text {
			if j == 0 {
				legend[index] = true
			} else {
				legend[index] = false
			}
			ids[index] = int(message.Id)
			content = append(content, line)
			index++
		}
		// margin, razen na čisto zadnjem
		if i != len(messages)-1 {
			for j := 0; j < messageItemMarginBottom; j++ {
				line := getFillWithString(m, contentWidth-2*contnetPadddingSides, " ")
				content = append(content, line)
				ids[index] = int(message.Id)
				legend[index] = false
				index++
			}
		}

	}

	return content, ids, legend
}

// pomozna
/*func getIstart(messages []client.UiMessageItem) int {
	rez := len(messages) - 1

	currHeight := 0
	maxHeight := contentHeight - 2*contnetPadddingTopBottom
	for i := len(messages) - 1; i >= 0; i-- {
		// margin
		if currHeight < maxHeight {
			currHeight += messageItemMarginBottom
		} else {
			break
		}
		// ime userja
		if currHeight < maxHeight {
			currHeight++
		} else {
			break
		}
		// lines
		for j := len(messages[i].Text) - 1; j >= 0; j-- {
			if currHeight < maxHeight {
				currHeight++
			} else {
				break
			}
		}
	}
}*/

func connectCmd(username string, head string, tail string) tea.Cmd {
	return func() tea.Msg {
		c, err := client.Client(username, head, tail)
		return connectResultMsg{
			clientState: c,
			err:         err,
		}
	}
}

func listTopicsCmd(m model) tea.Cmd {
	return func() tea.Msg {
		topics, err := client.GetTopics(m.client.clientState)
		if len(topics) > contentHeight-2*contnetPadddingTopBottom {
			m.contentEndIndexes[0] = contentHeight - 2*contnetPadddingTopBottom - 1
		} else {
			m.contentEndIndexes[0] = len(topics) - 1
		}
		return listTopicsMsg{
			topics: topics,
			err:    err,
		}
	}
}

func listMessagesCmd(m model) tea.Cmd {
	return func() tea.Msg {
		messages, err := client.ListMessages(m.client.clientState, m.openTopicId)
		// formatiramo text v vrstice
		messages = formatMessageItemsText(messages)
		return listMessagesMsg{
			messages: messages,
			err:      err,
		}
	}
}

func createTopicCmd(m model, name string) tea.Cmd {
	return func() tea.Msg {
		err := client.CreateTopic(m.client.clientState, name)
		return createTopicMsg{err: err}
	}
}

func likeMessageCmd(m model, messageId int) tea.Cmd {
	return func() tea.Msg {
		err := client.LikeMessage(m.client.clientState, int64(messageId), int(m.openTopicId))
		return likeMessageMsg{
			messageId: messageId,
			err:       err,
		}
	}
}

func deleteMessageCmd(m model, messageId int) tea.Cmd {
	return func() tea.Msg {
		err := client.DeleteMessage(m.client.clientState, int64(messageId), int(m.openTopicId))
		return deleteMessageMsg{
			messageId: messageId,
			err:       err,
		}
	}
}

func editMessageCmd(m model, messageId int, text string) tea.Cmd {
	return func() tea.Msg {
		err := client.EditMessage(m.client.clientState, messageId, text)
		msg := m.messages[int64(messageId)]
		return postNewMessageMsg{
			message: &msg,
			err:     err,
		}
	}
}

func postNewMessageCmd(m model, message string) tea.Cmd {
	return func() tea.Msg {
		message, err := client.PostMessage(m.client.clientState, m.openTopicId, message)
		return postNewMessageMsg{
			message: message,
			err:     err,
		}
	}
}

// pomozna
func digits(n int64) int {
	if n == 0 {
		return 1
	}
	if n < 0 {
		n = -n
	}

	count := 0
	for n > 0 {
		n /= 10
		count++
	}
	return count
}

func formatMessageItemsText(messages map[int64]client.UiMessageItem) map[int64]client.UiMessageItem {
	for id, message := range messages {
		message.Text = wrapText(message.Text[0], messageItemWidth)
		messages[id] = message
	}
	return messages
}

func wrapText(text string, width int) []string {
	var lines []string
	var currentLine string
	currentWidth := 0

	words := strings.Fields(text)

	for _, word := range words {
		wordWidth := runewidth.StringWidth(word)

		if currentLine == "" {
			currentLine = word
			currentWidth = wordWidth
			continue
		}

		if currentWidth+1+wordWidth <= width {
			currentLine += " " + word
			currentWidth += 1 + wordWidth
		} else {
			// zapolni do točne širine
			for currentWidth < width {
				currentLine += " "
				currentWidth++
			}
			lines = append(lines, currentLine)

			currentLine = word
			currentWidth = wordWidth
		}
	}

	if currentLine != "" {
		for currentWidth < width {
			currentLine += " "
			currentWidth++
		}
		lines = append(lines, currentLine)
	}

	return lines
}

func getLastMessageCursorIndex(m model) int {
	if len(m.messages) == 0 {
		return 0
	}

	// messages -> slice
	messages := make([]client.UiMessageItem, 0, len(m.messages))
	for _, msg := range m.messages {
		messages = append(messages, msg)
	}

	// sortiramo po timestamp
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.AsTime().Before(messages[j].Timestamp.AsTime())
	})

	content, messageIds, _ := buildContentMessages(m, messages)

	// zadnji index, ki pripada zadnjem message-u
	lastMsgId := int(messages[len(messages)-1].Id)

	for i := len(content) - 1; i >= 0; i-- {
		if messageIds[i] == lastMsgId {
			return i
		}
	}

	return len(content) - 1
}

func getSelectedMessageId(m model) int {
	messages := make([]client.UiMessageItem, 0, len(m.messages))
	for _, msg := range m.messages {
		messages = append(messages, msg)
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.AsTime().Before(messages[j].Timestamp.AsTime())
	})

	_, messageIds, _ := buildContentMessages(m, messages)
	return messageIds[m.cursorMessagesIndex]
}

func RunUI() {
	m := initialModel()
	if _, err := tea.NewProgram(m).Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}
