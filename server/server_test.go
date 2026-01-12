package server

import (
	"context"
	"testing"

	pb "razpravljalnica/proto"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPostMessage(t *testing.T) {
	server := newTestServerWithUserAndTopic(t)

	req := &pb.PostMessageRequest{
		UserId:  1,
		TopicId: 1,
		Text:    "hello world",
	}

	msg, err := server.PostMessage(context.Background(), req)
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

	// preverimo da je message res shranjen v server.messages
	if len(server.messages) != 1 {
		t.Errorf("expected 1 message stored, got %d", len(server.messages))
	}
}

func TestCreateTopic(t *testing.T) {
	server := newMessageBoardServer("test", "node-1", true, true)
	user := server.createUser(t, "janez")

	req := &pb.CreateTopicRequest{
		Name:   "new-topic",
		UserId: user.Id,
	}

	topic, err := server.CreateTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}

	if topic == nil {
		t.Fatalf("expected topic, got nil")
	}

	if topic.Name != req.Name {
		t.Errorf("topic name mismatch: got %q want %q", topic.Name, req.Name)
	}

	if len(server.topics) != 1 {
		t.Errorf("expected 1 topic stored, got %d", len(server.topics))
	}
}

func TestLikeMessage(t *testing.T) {
	server := newTestServerWithUserAndTopic(t)

	msg, err := server.PostMessage(context.Background(), &pb.PostMessageRequest{
		UserId:  1,
		TopicId: 1,
		Text:    "hello",
	})
	if err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}

	_, err = server.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		MessageId: msg.Id,
		UserId:    1,
	})
	if err != nil {
		t.Fatalf("LikeMessage failed: %v", err)
	}

	storedMsg := server.messages[msg.Id]
	if storedMsg.Likes != 1 {
		t.Errorf("expected 1 like, got %d", storedMsg.Likes)
	}

	// preverimo ali lahko šeenkrat lajkamo
	_, err = server.LikeMessage(context.Background(), &pb.LikeMessageRequest{
		MessageId: msg.Id,
		UserId:    1,
	})
	if err == nil {
		t.Fatalf("message was liked twice by the same user")
	}

	storedMsg = server.messages[msg.Id]
	if storedMsg.Likes != 1 {
		t.Errorf("expected 1 like, got %d", storedMsg.Likes)
	}
}

func TestUpdateMessage(t *testing.T) {
	server := newTestServerWithUserAndTopic(t)

	msg, err := server.PostMessage(context.Background(), &pb.PostMessageRequest{
		UserId:  1,
		TopicId: 1,
		Text:    "old text",
	})
	if err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}

	newText1 := "edited text 1"

	_, err = server.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		MessageId: msg.Id,
		Text:      newText1,
		UserId:    1,
	})
	if err != nil {
		t.Fatalf("EditMessage failed: %v", err)
	}

	updated := server.messages[msg.Id]
	if updated.Text != newText1 {
		t.Errorf("message text not updated: got %q want %q", updated.Text, newText1)
	}

	// unauthorised edit test
	newText2 := "edited text 2"

	_, err = server.UpdateMessage(context.Background(), &pb.UpdateMessageRequest{
		MessageId: msg.Id,
		Text:      newText2,
		UserId:    2,
	})
	if err == nil {
		t.Fatalf("message edited by user who is not the author")
	}

	updated = server.messages[msg.Id]
	if updated.Text != newText1 {
		t.Errorf("message text not updated: got %q want %q", updated.Text, newText1)
	}
}

func TestDeleteMessage(t *testing.T) {
	server := newTestServerWithUserAndTopic(t)

	msg1, err := server.PostMessage(context.Background(), &pb.PostMessageRequest{
		UserId:  1,
		TopicId: 1,
		Text:    "to be deleted",
	})
	if err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}

	_, err = server.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		MessageId: msg1.Id,
		UserId:    1,
	})
	if err != nil {
		t.Fatalf("DeleteMessage failed: %v", err)
	}

	if _, ok := server.messages[msg1.Id]; ok {
		t.Errorf("message still present after delete")
	}

	// unauthorised delete attempt
	msg2, err := server.PostMessage(context.Background(), &pb.PostMessageRequest{
		UserId:  1,
		TopicId: 1,
		Text:    "to be deleted",
	})
	if err != nil {
		t.Fatalf("PostMessage failed: %v", err)
	}

	_, err = server.DeleteMessage(context.Background(), &pb.DeleteMessageRequest{
		MessageId: msg2.Id,
		UserId:    2,
	})
	if err == nil {
		t.Fatalf("message deleted by user who is not the author")
	}

	if _, ok := server.messages[msg2.Id]; !ok {
		t.Errorf("message deleted by user who is not the author")
	}
}

func TestCreateUser(t *testing.T) {
	server := newMessageBoardServer("test", "node-1", true, true)

	req := &pb.CreateUserRequest{
		Name: "janez",
	}

	// prvi create
	user1, err := server.CreateUser(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}

	if user1 == nil {
		t.Fatalf("expected user, got nil")
	}

	if user1.Name != req.Name {
		t.Errorf("user name mismatch: got %q want %q", user1.Name, req.Name)
	}

	if user1.Id != 1 {
		t.Errorf("expected user id 1, got %d", user1.Id)
	}

	if len(server.users) != 1 {
		t.Errorf("expected 1 user stored, got %d", len(server.users))
	}

	// drugi create z istim imenom
	user2, err := server.CreateUser(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateUser (duplicate) returned error: %v", err)
	}

	if user2.Id != user1.Id {
		t.Errorf("duplicate user created: got id %d want %d", user2.Id, user1.Id)
	}

	if len(server.users) != 1 {
		t.Errorf("expected still 1 user stored, got %d", len(server.users))
	}
}

func TestListTopics(t *testing.T) {
	server := newMessageBoardServer("test", "node-1", true, true)

	// prazen seznam
	res, err := server.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	if res == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(res.Topics) != 0 {
		t.Errorf("expected 0 topics, got %d", len(res.Topics))
	}

	// dodamo 2 topica
	topic1 := server.createTopic(t, "topic-1")
	topic2 := server.createTopic(t, "topic-2")

	res, err = server.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics returned error: %v", err)
	}
	if len(res.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(res.Topics))
	}

	// preverimo vsebino
	storedTopics := make(map[int64]string)
	for _, topic := range res.Topics {
		storedTopics[topic.Id] = topic.Name
	}

	expected := make(map[int64]string)
	expected[topic1.Id] = topic1.Name
	expected[topic2.Id] = topic2.Name

	for id, name := range expected {
		if storedTopics[id] != name {
			t.Errorf("topic mismatch for id %d: got %q want %q", id, storedTopics[id], name)
		}
	}
}

func TestGetMessages(t *testing.T) {
	server := newTestServerWithUserAndTopic(t)

	// ni še sporočil
	res, err := server.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: 1,
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

	// dodamo sporočila
	text1 := "message 1"
	text2 := "message 2"

	server.postMessage(t, 1, 1, text1)
	server.postMessage(t, 1, 1, text2)

	storedMessages, err := server.GetMessages(context.Background(), &pb.GetMessagesRequest{
		TopicId: 1,
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	if storedMessages == nil {
		t.Fatalf("expected response, got nil")
	}
	if len(storedMessages.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(storedMessages.Messages))
	}

	if storedMessages.Messages[0].Id != 1 {
		t.Errorf("expected MessageId 1, got %d", storedMessages.Messages[0].Id)
	}
	if storedMessages.Messages[1].Id != 2 {
		t.Errorf("expected MessageId 2, got %d", storedMessages.Messages[1].Id)
	}

	if storedMessages.Messages[0].TopicId != 1 {
		t.Errorf("expected TopicId 1, got %d", storedMessages.Messages[0].TopicId)
	}
	if storedMessages.Messages[1].TopicId != 1 {
		t.Errorf("expected TopicId 1, got %d", storedMessages.Messages[1].TopicId)
	}

	if storedMessages.Messages[0].Text != text1 {
		t.Errorf("expected Text %s, got %s", text1, storedMessages.Messages[0].Text)
	}
	if storedMessages.Messages[1].Text != text2 {
		t.Errorf("expected Text %s, got %s", text2, storedMessages.Messages[1].Text)
	}

	if storedMessages.Messages[0].UserId != 1 {
		t.Errorf("expected UserId 1, got %d", storedMessages.Messages[0].UserId)
	}
	if storedMessages.Messages[1].UserId != 1 {
		t.Errorf("expected UserId 1, got %d", storedMessages.Messages[1].UserId)
	}
}

// helpers

func (server *MessageBoardServer) createUser(t *testing.T, username string) *pb.User {
	t.Helper()
	user := &pb.User{
		Id:   server.nextUserID,
		Name: username,
	}
	server.users[user.Id] = user
	server.nextUserID++
	return user
}

func (server *MessageBoardServer) createTopic(t *testing.T, name string) *pb.Topic {
	t.Helper()
	topic := &pb.Topic{
		Id:   server.nextTopicID,
		Name: name,
	}
	server.topics[topic.Id] = topic
	server.nextTopicID++
	return topic
}

func (server *MessageBoardServer) postMessage(t *testing.T, userId int64, topicId int64, text string) *pb.Message {
	t.Helper()
	message := &pb.Message{
		Id:        server.nextMessageID,
		UserId:    userId,
		TopicId:   topicId,
		Text:      text,
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}
	server.messages[message.Id] = message
	server.nextMessageID++
	return nil
}

func newTestServerWithUserAndTopic(t *testing.T) *MessageBoardServer {
	t.Helper()

	server := newMessageBoardServer("test", "node-1", true, true)

	server.createUser(t, "janez")
	server.createTopic(t, "test-topic")

	return server
}
