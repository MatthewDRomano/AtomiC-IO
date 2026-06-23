# CC and Compiler Flags
CC        := gcc
CFLAGS    := -Wall -Wextra -O2 -Iinclude -pthread
AR        := ar rcs

# Directories
SRC_DIR   := src
INC_DIR   := include
OBJ_DIR   := obj
BIN_DIR   := bin

# Library Target Name
LIB_NAME  := libatomicio.a
TARGET_LIB := $(BIN_DIR)/$(LIB_NAME)

# Core Library Source and Object Files (Your 4 source files)
LIB_SRCS  := $(SRC_DIR)/at_net.c \
             $(SRC_DIR)/atomicio.c \
             $(SRC_DIR)/atomicio_cl.c \
             $(SRC_DIR)/log.c
LIB_OBJS  := $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(LIB_SRCS))

# Automatically find all individual files in examples/ and tests/
EXAMPLES_SRCS := $(wildcard examples/*.c)
EXAMPLES_BINS := $(patsubst examples/%.c, $(BIN_DIR)/%, $(EXAMPLES_SRCS))

TESTS_SRCS    := $(wildcard tests/*.c)
TESTS_BINS    := $(patsubst tests/%.c, $(BIN_DIR)/%, $(TESTS_SRCS))

# System Installation Paths (Standard for linux/macOS)
PREFIX   ?= /usr/local
LIBDIR   := $(PREFIX)/lib
INCDIR   := $(PREFIX)/include/atomicio

# .PHONY Targets
.PHONY: all clean examples tests install uninstall

# Default target builds everything locally
all: $(TARGET_LIB) examples tests

# 1. Archive the object files into the Static Library
$(TARGET_LIB): $(LIB_OBJS) | $(BIN_DIR)
	$(AR) $@ $^

# Compile core library .c files into .o files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# 2. Compile Examples and link them dynamically to the local static library
examples: $(EXAMPLES_BINS)

$(BIN_DIR)/%: examples/%.c $(TARGET_LIB)
	$(CC) $(CFLAGS) $< -L$(BIN_DIR) -l:$(LIB_NAME) -o $@

# 3. Compile Tests and link them dynamically to the local static library
tests: $(TESTS_BINS)

$(BIN_DIR)/%: tests/%.c $(TARGET_LIB)
	$(CC) $(CFLAGS) $< -L$(BIN_DIR) -l:$(LIB_NAME) -o $@

# Build directories helper
$(OBJ_DIR) $(BIN_DIR):
	mkdir -p $@

# 4. Global Installation Phase
install: $(TARGET_LIB)
	@echo "Installing library to $(LIBDIR) and headers to $(INCDIR)..."
	mkdir -p $(LIBDIR)
	mkdir -p $(INCDIR)
	cp $(TARGET_LIB) $(LIBDIR)/
	cp $(INC_DIR)/*.h $(INCDIR)/
	@echo "Installation complete!"

# 5. Clean installation from system
uninstall:
	@echo "Removing library and headers from system..."
	rm -f $(LIBDIR)/$(LIB_NAME)
	rm -rf $(INCDIR)
	@echo "Uninstalled successfully."

# Local cleanup
clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)
