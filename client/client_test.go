package client

import (
	"context"
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
	/*getReq := &pb.GetMessagesRequest{
		TopicId: req.TopicId,
	}
	getResp, err := clientState.rpcHead.GetMessages(clientState.Ctx, getReq)
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}*/
	messages := getMessages(t, clientState, req.TopicId)
	found := false
	for _, m := range messages {
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

func TestLikeMessage(t *testing.T) {
	// naredimo 2 uporabnika
	clientState1, err := ConnectToServer("localhost:8000", "test like message user 1")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState1.ConnHead.Close()
	defer clientState1.ConnTail.Close()

	clientState2, err := ConnectToServer("localhost:8000", "test like message user 2")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState2.ConnHead.Close()
	defer clientState2.ConnTail.Close()

	// naredimo topic in sporočilo
	topic := createTopic(t, clientState1, "test like message topic")
	text1 := "test text 1"
	msgPosted := postMessage(t, clientState1, topic.Id, text1)

	// likeamo
	msgRecived1, err := clientState2.rpcHead.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		MessageId: msgPosted.Id,
		UserId:    clientState2.User.Id,
	})
	if err != nil {
		t.Fatalf("LikeMessage failed: %v", err)
	}

	// likeamo ponovno (nebi smelo pustit)
	_, err = clientState2.rpcHead.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		MessageId: msgPosted.Id,
		UserId:    clientState2.User.Id,
	})
	if err == nil {
		t.Fatalf("Message liked twice by the same user")
	}

	// poskus likea sam svojega sporočila
	_, err = clientState1.rpcHead.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		MessageId: msgPosted.Id,
		UserId:    clientState1.User.Id,
	})
	if err == nil {
		t.Fatalf("Message liked by author")
	}

	// preverjanje št likeov
	messages := getMessages(t, clientState1, topic.Id)
	index := -1
	found := false
	for i, m := range messages {
		if m.Id == msgRecived1.Id {
			found = true
			index = i
			break
		}
	}
	if !found {
		t.Errorf("posted message with ID %d not found in GetMessages response", msgPosted.Id)
	}
	if messages[index].Likes != 1 {
		t.Errorf("expected 1 like, got %d", messages[index].Likes)
	}
}

func TestUpdateMessage(t *testing.T) {

	// naredimo 2 uporabnika
	clientState1, err := ConnectToServer("localhost:8000", "test update message user 1")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState1.ConnHead.Close()
	defer clientState1.ConnTail.Close()

	clientState2, err := ConnectToServer("localhost:8000", "test update message user 2")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState2.ConnHead.Close()
	defer clientState2.ConnTail.Close()

	// naredimo topic in sporočilo
	topic := createTopic(t, clientState1, "test update message topic")
	text1 := "test text 1"
	msgPosted := postMessage(t, clientState1, topic.Id, text1)

	newText1 := "edited text 1"

	updated, err := clientState1.rpcHead.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		MessageId: msgPosted.Id,
		Text:      newText1,
		UserId:    clientState1.User.Id,
	})
	if err != nil {
		t.Fatalf("EditMessage failed: %v", err)
	}
	if updated.Text != newText1 {
		t.Errorf("message text not updated: got %s want %s", updated.Text, newText1)
	}

	// unauthorised edit test
	newText2 := "edited text 2"

	_, err = clientState2.rpcHead.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		MessageId: msgPosted.Id,
		Text:      newText2,
		UserId:    clientState2.User.Id,
	})
	if err == nil {
		t.Fatalf("message edited by user who is not the author")
	}

	messages := getMessages(t, clientState1, topic.Id)
	index := -1
	found := false
	for i, m := range messages {
		if m.Id == msgPosted.Id {
			found = true
			index = i
			break
		}
	}
	if !found {
		t.Errorf("posted message with ID %d not found in GetMessages response", msgPosted.Id)
	}
	if messages[index].Text == newText2 {
		t.Errorf("message edited by user who is not the author")
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

func postMessage(t *testing.T, clientState *ClientState, topicId int64, text string) *pb.Message {
	t.Helper()
	req := &pb.PostMessageRequest{
		UserId:  clientState.User.Id,
		TopicId: topicId,
		Text:    text,
	}
	message, err := clientState.rpcHead.PostMessage(clientState.Ctx, req)
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
		return nil
	}
	return message
}

func getMessages(t *testing.T, clientState *ClientState, topicId int64) []*pb.Message {
	getReq := &pb.GetMessagesRequest{
		TopicId: topicId,
	}
	getResp, err := clientState.rpcHead.GetMessages(clientState.Ctx, getReq)
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	return getResp.Messages
}

func getTopics(t *testing.T, clientState *ClientState) []*pb.Topic {
	t.Helper()
	resp, err := clientState.rpcHead.ListTopics(clientState.Ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	return resp.Topics
}
