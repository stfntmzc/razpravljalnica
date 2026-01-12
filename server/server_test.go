package server

import (
	"context"
	"testing"

	pb "razpravljalnica/proto"
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

func newTestServerWithUserAndTopic(t *testing.T) *MessageBoardServer {
	t.Helper()

	server := newMessageBoardServer("test", "node-1", true, true)

	server.createUser(t, "janez")
	server.createTopic(t, "test-topic")

	return server
}
