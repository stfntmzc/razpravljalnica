package client

import (
	pb "razpravljalnica/proto"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"
)

func TestPostMessage(t *testing.T) {
	clientState, err := ConnectToServer("localhost:8000", "test post message user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState.ConnHead.Close()
	defer clientState.ConnTail.Close()

	topic := createTopic(t, clientState, "test post message topic")

	req := &pb.PostMessageRequest{
		UserId:  clientState.User.Id,
		TopicId: topic.Id,
		Text:    "hello world",
	}

	msg, err := clientState.rpcHead.PostMessage(clientState.Ctx, req)
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}
	// preverimo da je message ustvarjen
	if msg == nil {
		t.Fatalf("expected message, got nil")
	}

	// preverimo da je vsebina ok
	if msg.Text != req.Text {
		t.Errorf("message text mismatch: got %q want %q", msg.Text, req.Text)
	}

	// preverimo da se user ujema
	if msg.UserId != req.UserId {
		t.Errorf("userId mismatch: got %d want %d", msg.UserId, req.UserId)
	}

	// prevermo da se topic id ujema
	if msg.TopicId != req.TopicId {
		t.Errorf("topicId mismatch: got %d want %d", msg.TopicId, req.TopicId)
	}

	// preverimo da je message shranjen na serverju z GetMessages
	getReq := &pb.GetMessagesRequest{
		TopicId: req.TopicId,
	}
	getResp, err := clientState.rpcHead.GetMessages(clientState.Ctx, getReq)
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	found := false
	for _, m := range getResp.Messages {
		if m.Id == msg.Id {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("posted message with ID %d not found in GetMessages response", msg.Id)
	}
}

func TestCreateTopic(t *testing.T) {

	clientState, err := ConnectToServer("localhost:8000", "test create topic user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState.ConnHead.Close()
	defer clientState.ConnTail.Close()

	prevTopics := getTopics(t, clientState)

	req := &pb.CreateTopicRequest{
		Name:   "test create topic topic",
		UserId: clientState.User.Id,
	}
	topic, err := clientState.rpcHead.CreateTopic(clientState.Ctx, req)
	if err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}
	if topic == nil {
		t.Fatalf("expected topic, got nil")
	}

	// ujemanje imena
	if topic.Name != req.Name {
		t.Errorf("topic name mismatch: got %q want %q", topic.Name, req.Name)
	}

	// preverimo da je topic dodan na server
	newTopics := getTopics(t, clientState)
	if len(newTopics) != len(prevTopics)+1 {
		t.Errorf("expected %d topics after creation, got %d", len(prevTopics)+1, len(newTopics))
	}
	found := false
	for _, t := range newTopics {
		if t.Id == topic.Id {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("created topic with ID %d not found in topic list", topic.Id)
	}
}

// helpers

func createTopic(t *testing.T, clientState *ClientState, name string) *pb.Topic {
	t.Helper()
	req := &pb.CreateTopicRequest{
		Name:   name,
		UserId: clientState.User.Id,
	}
	topic, err := clientState.rpcHead.CreateTopic(clientState.Ctx, req)
	if err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}
	return topic
}

func getTopics(t *testing.T, clientState *ClientState) []*pb.Topic {
	t.Helper()
	resp, err := clientState.rpcHead.ListTopics(clientState.Ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	return resp.Topics
}
