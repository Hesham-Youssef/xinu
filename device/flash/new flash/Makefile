# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -g -Iinclude

CFLAGS += -fsanitize=address -fno-omit-frame-pointer
LDFLAGS += -fsanitize=address


# Project name and source files
TARGET = test
SRCS = mapping_table.c page_cache.c lss.c main.c
OBJS = $(SRCS:.c=.o)

# Build folder
BUILD_DIR = build

# Object files in build folder
OBJS = $(SRCS:%.c=$(BUILD_DIR)/%.o)

# Default target
all: $(BUILD_DIR)/$(TARGET)

# Linking step (make sure build dir exists)
$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compilation step for .c to .o (create build dir if missing)
$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Create the build folder if it doesn't exist
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Clean up build files
clean:
	rm -rf $(BUILD_DIR)

# Rebuild everything
rebuild: clean all

# Phony targets
.PHONY: all clean rebuild asan