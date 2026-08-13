server:
	nodemon --watch './**/*.go' --signal SIGTERM --exec 'go' run cmd/server/main.go
