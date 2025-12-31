package main

import (
	"fmt"
	"os"
	"razpravljalnica/client"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
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

	tabs    []string
	openTab string

	// client
	client connectResultMsg
}

type connectResultMsg struct {
	clientState *client.ClientState
	err         error
}

func initialModel() model {
	inputs := make([]textinput.Model, 3)

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

	return model{
		inputs: inputs,
		tabs:   []string{"Topics", "Live chat"},
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
			// pocezava vspostavljena
			m.client.clientState = msg.clientState
			m.loggedIn = true
			return m, nil
		}

		var cmd tea.Cmd
		if !m.loggedIn {
			m.inputs[m.focus], cmd = m.inputs[m.focus].Update(msg)
		}
		return m, cmd
	}

	if m.loggedIn {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q":
				m.quitting = true
				return m, tea.Quit
			}
		}

	}

	return m, nil
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
	return loginView(m)
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

func connectCmd(username string, head string, tail string) tea.Cmd {
	return func() tea.Msg {
		c, err := client.Client(username, head, tail)
		return connectResultMsg{
			clientState: c,
			err:         err,
		}
	}
}

func RunUI() {
	m := initialModel()
	if _, err := tea.NewProgram(m).Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}
