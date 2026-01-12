package client

import (
	"context"
	pb "razpravljalnica/proto"
	"sort"
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

func TestDeleteMessage(t *testing.T) {
	// naredimo 2 uporabnika
	clientState1, err := ConnectToServer("localhost:8000", "test delete message user 1")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState1.ConnHead.Close()
	defer clientState1.ConnTail.Close()

	clientState2, err := ConnectToServer("localhost:8000", "test delete message user 2")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState2.ConnHead.Close()
	defer clientState2.ConnTail.Close()

	// naredimo topic in sporočilo
	topic := createTopic(t, clientState1, "test delete message topic")
	text1 := "test text 1"
	msgPosted := postMessage(t, clientState1, topic.Id, text1)

	// poskušamo brisat z drugim uporabnikom
	_, err = clientState2.rpcHead.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		MessageId: msgPosted.Id,
		UserId:    clientState2.User.Id,
	})
	if err == nil {
		t.Fatalf("error deleting with wrong user: %s", err)
	}
	// preverimo če je bil zvrisan
	messages := getMessages(t, clientState1, topic.Id)
	found := false
	for _, m := range messages {
		if m.Id == msgPosted.Id {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("message deleted by user who is not the author")
	}

	// zbrišemo z pravim uporabnikom
	_, err = clientState1.rpcHead.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		MessageId: msgPosted.Id,
		UserId:    clientState1.User.Id,
	})
	if err != nil {
		t.Fatalf("error deleting message: %v", err)
	}

	// preverimo ali je zbrisan
	messages = getMessages(t, clientState1, topic.Id)
	found = false
	for _, m := range messages {
		if m.Id == msgPosted.Id {
			found = true
			break
		}
	}
	if found {
		t.Fatalf("message not deleted")
	}
}

func TestCreateUser(t *testing.T) {
	// prvi create
	clientState1, err := ConnectToServer("localhost:8000", "test create user user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState1.ConnHead.Close()
	defer clientState1.ConnTail.Close()

	// druge create (login)
	clientState2, err := ConnectToServer("localhost:8000", "test create user user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState2.ConnHead.Close()
	defer clientState2.ConnTail.Close()

	// preverimo id in ime
	if clientState1.User.Id != clientState2.User.Id {
		t.Errorf("user id mismatch: got %d want %d", clientState1.User.Id, clientState2.User.Id)
	}
	if clientState1.User.Name != clientState2.User.Name {
		t.Errorf("user name mismatch: got %s want %s", clientState1.User.Name, clientState2.User.Name)
	}
}

func TestListTopics(t *testing.T) {

	clientState, err := ConnectToServer("localhost:8000", "test list topics user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState.ConnHead.Close()
	defer clientState.ConnTail.Close()

	//še brez novih topicov
	res, err := clientState.rpcTail.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected response, got nil")
	}
	lenPrev := len(res.Topics)

	// dodamo 2 topica
	topic1 := createTopic(t, clientState, "topic-1")
	topic2 := createTopic(t, clientState, "topic-2")

	res, err = clientState.rpcTail.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	if len(res.Topics) != lenPrev+2 {
		t.Fatalf("expected %d topics, got %d", lenPrev+2, len(res.Topics))
	}

	// sortiramo
	sort.Slice(res.Topics, func(i, j int) bool {
		return res.Topics[i].Id < res.Topics[j].Id
	})

	// preverimo id
	if res.Topics[lenPrev+0].Id != topic1.Id {
		t.Errorf("expected topic id 1, got %d", res.Topics[lenPrev+0].Id)
	}
	if res.Topics[lenPrev+1].Id != topic2.Id {
		t.Errorf("expected topic id 2, got %d", res.Topics[lenPrev+1].Id)
	}
	// preverimo imena
	if res.Topics[lenPrev+0].Name != topic1.Name {
		t.Errorf("expected topic name %s, got %s", topic1.Name, res.Topics[lenPrev+0].Name)
	}
	if res.Topics[lenPrev+1].Name != topic2.Name {
		t.Errorf("expected topic name %s, got %s", topic2.Name, res.Topics[lenPrev+1].Name)
	}
}

func TestGetMessages(t *testing.T) {
	clientState, err := ConnectToServer("localhost:8000", "test get messages user")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientState.ConnHead.Close()
	defer clientState.ConnTail.Close()

	// naredimo topic
	topic := createTopic(t, clientState, "test get messages topic")

	// ni še sporočil
	res, err := clientState.rpcTail.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic.Id,
	})
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(res.Messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(res.Messages))
	}

	// dodamo 2 sporočila
	text1 := "message 1"
	text2 := "message 2"
	msg1 := postMessage(t, clientState, topic.Id, text1)
	msg2 := postMessage(t, clientState, topic.Id, text2)

	messages, err := clientState.rpcTail.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: topic.Id,
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	if messages == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(messages.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages.Messages))
	}

	// sprtiramo
	sort.Slice(messages.Messages, func(i, j int) bool {
		return messages.Messages[i].Id < messages.Messages[j].Id
	})

	// preverjamo id
	if messages.Messages[0].Id != msg1.Id {
		t.Errorf("expected MessageId %d, got %d", msg1.Id, messages.Messages[0].Id)
	}
	if messages.Messages[1].Id != msg2.Id {
		t.Errorf("expected MessageId %d, got %d", msg2.Id, messages.Messages[1].Id)
	}

	// preverjamo topic
	if messages.Messages[0].TopicId != topic.Id {
		t.Errorf("expected TopicId %d, got %d", topic.Id, messages.Messages[0].TopicId)
	}
	if messages.Messages[1].TopicId != topic.Id {
		t.Errorf("expected TopicId %d, got %d", topic.Id, messages.Messages[1].TopicId)
	}

	// preverjamo vsebino
	if messages.Messages[0].Text != text1 {
		t.Errorf("expected Text %s, got %s", text1, messages.Messages[0].Text)
	}
	if messages.Messages[1].Text != text2 {
		t.Errorf("expected Text %s, got %s", text2, messages.Messages[1].Text)
	}

	// preverjamo uporabnika
	if messages.Messages[0].UserId != clientState.User.Id {
		t.Errorf("expected UserId 1, got %d", messages.Messages[0].UserId)
	}
	if messages.Messages[1].UserId != clientState.User.Id {
		t.Errorf("expected UserId 1, got %d", messages.Messages[1].UserId)
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
