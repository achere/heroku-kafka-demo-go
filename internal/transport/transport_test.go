package transport

import (
	"testing"
	"time"
)

func TestMessageBuffer(t *testing.T) {
	t.Run("SaveMessage", func(t *testing.T) {
		mb := MessageBuffer{
			MaxSize: 10,
		}

		msg := Message{
			Metadata: MessageMetadata{
				ReceivedAt: time.Now(),
			},
			Value:     "test",
			Partition: 0,
			Offset:    0,
		}

		mb.SaveMessage(msg)

		if len(mb.receivedMessages) != 1 {
			t.Errorf("Expected 1 message, got %d", len(mb.receivedMessages))
		}

		if mb.receivedMessages[0].Value != "test" {
			t.Errorf("Expected message value to be 'test', got %s", mb.receivedMessages[0].Value)
		}
	})

	t.Run("SaveMessage with MaxSize", func(t *testing.T) {
		mb := MessageBuffer{
			MaxSize: 2,
		}

		msg := Message{
			Metadata: MessageMetadata{
				ReceivedAt: time.Now(),
			},
			Value:     "test",
			Partition: 0,
			Offset:    0,
		}

		mb.SaveMessage(msg)
		mb.SaveMessage(msg)
		mb.SaveMessage(msg)

		if len(mb.receivedMessages) != 2 {
			t.Errorf("Expected 2 messages, got %d", len(mb.receivedMessages))
		}

		if mb.receivedMessages[0].Value != "test" {
			t.Errorf("Expected message value to be 'test', got %s", mb.receivedMessages[0].Value)
		}
	})
}

func TestMessageHandler(t *testing.T) {
	t.Run("Setup", func(t *testing.T) {
		handler := MessageHandler{
			Ready:  make(chan bool),
			buffer: &MessageBuffer{},
		}

		err := handler.Setup(nil)
		if err != nil {
			t.Errorf("Expected error to be nil, got %s", err)
		}

		<-handler.Ready
	})

	t.Run("Cleanup", func(t *testing.T) {
		handler := MessageHandler{
			Ready:  make(chan bool),
			buffer: &MessageBuffer{},
		}

		err := handler.Cleanup(nil)
		if err != nil {
			t.Errorf("Expected error to be nil, got %s", err)
		}
	})
}
