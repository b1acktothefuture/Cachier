# Variables
BUILD_DIR := build
CMD_DIR := cmd
PROTO_DIR:= proto
PROTO_GEN_DIR:= gen

# Find all subdirectories in cmd/ that contain a main.go file
SUBDIRS := $(shell find $(CMD_DIR) -type f -name main.go | xargs -n 1 dirname)

# Generate build targets for each subdirectory
BINARIES := $(patsubst $(CMD_DIR)/%, $(BUILD_DIR)/%, $(SUBDIRS))

# Default target
.PHONY: all
all: $(BINARIES)

# Rule to build each binary
$(BUILD_DIR)/%: $(CMD_DIR)/%/main.go proto
	mkdir -p $(@D)
	go build -o $@ ./$(CMD_DIR)/$*

.PHONY: proto
proto: $(PROTO_DIR)/
	protoc --go_out=. --go-grpc_out=. $(PROTO_DIR)/*


# Clean build directory
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

# Utility target to list binaries
.PHONY: list
list:
	@echo "Binaries to build:"
	@echo $(BINARIES)
