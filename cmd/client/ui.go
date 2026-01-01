package main

import (
	"fmt"
	"os"
	"razpravljalnica/client"
	"sort"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
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

	messagesItemWidth = 72

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
	openTopicId int
	messages    map[int64]messageItem

	// client
	client connectResultMsg
}

type messageItem struct {
	username string
	text     string
}

type connectResultMsg struct {
	clientState *client.ClientState
	err         error
}

type listTopicsMsg struct {
	topics map[int64]string
	err    error
}

type createTopicMsg struct {
	id  int64
	err error
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
	for i := 0; i < marginLeft; i++ {
		s += " "
	}

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
		s += getTopicsString(m)
	}

	return s
}

func getTabsPadding(m model) string {
	return getFillWithString(m, tabsPadding, " ")
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

func createTopicCmd(m model, name string) tea.Cmd {
	return func() tea.Msg {
		err := client.CreateTopic(m.client.clientState, name)
		return createTopicMsg{err: err}
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

func RunUI() {
	m := initialModel()
	if _, err := tea.NewProgram(m).Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}
