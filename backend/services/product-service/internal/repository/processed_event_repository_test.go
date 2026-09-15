package repository

import (
	"errors"
	"testing"

	"ecomerce-service/services/product-service/internal/domain"

	"github.com/stretchr/testify/assert"
)

func TestProcessedEventRepository_InsertIfNew(t *testing.T) {
	db := setupTestDB(t)
	repo := NewProcessedEventRepository(db)

	consumerName := "test-consumer"
	eventID := "evt-12345"

	// 1. Insert first time -> success
	isNew, err := repo.InsertIfNew(db, consumerName, eventID)
	assert.NoError(t, err)
	assert.True(t, isNew)

	// Verify in DB
	var count int64
	db.Model(&domain.ProcessedEvent{}).Where("consumer_name = ? AND event_id = ?", consumerName, eventID).Count(&count)
	assert.Equal(t, int64(1), count)

	// 2. Insert duplicate -> should return ErrDuplicateEvent and isNew=false
	isNew2, err2 := repo.InsertIfNew(db, consumerName, eventID)
	assert.Error(t, err2)
	assert.True(t, errors.Is(err2, ErrDuplicateEvent))
	assert.False(t, isNew2)

	// Verify count is still 1
	db.Model(&domain.ProcessedEvent{}).Where("consumer_name = ? AND event_id = ?", consumerName, eventID).Count(&count)
	assert.Equal(t, int64(1), count)
}
