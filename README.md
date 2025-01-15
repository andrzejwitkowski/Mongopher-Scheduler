# Mongopher-Scheduler

This is a simple task scheduler for MongoDB written in Go. It is designed to be used in a distributed environment where multiple instances of the scheduler need to coordinate and process tasks.

## Features

- Simple and lightweight
- Supports both synchronous and asynchronous task processing
- Supports retry logic for recoverable tasks
- Supports custom task handlers
- Supports scheduled tasks
- Supports task history tracking

## Usage

### Installation

To install the Mongopher-Scheduler, you can use the following command:

```bash
go get github.com/mongopher/mongopher-scheduler
```