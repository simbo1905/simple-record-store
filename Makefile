# Simple Record Store Monorepo Makefile
# This Makefile provides convenient targets for building and testing packages

.PHONY: help build test clean all mmap mmap-test mmap-clean

# Default target
help:
	@echo "Simple Record Store Monorepo"
	@echo "============================"
	@echo ""
	@echo "Available targets:"
	@echo "  help        Show this help message"
	@echo "  build       Build all packages"
	@echo "  test        Test all packages"
	@echo "  clean       Clean all packages"
	@echo "  all         Build and test all packages"
	@echo ""
	@echo "Package-specific targets:"
	@echo "  mmap        Build the memory-mapped file package (Java)"
	@echo "  mmap-test   Test the memory-mapped file package"
	@echo "  mmap-clean  Clean the memory-mapped file package"
	@echo ""
	@echo "Options:"
	@echo "  VERBOSE=1   Enable verbose output"
	@echo "  CLEAN=1     Clean before building"
	@echo "  SKIP_TESTS=1 Skip running tests"
	@echo ""
	@echo "Examples:"
	@echo "  make mmap                    # Build mmap package"
	@echo "  make mmap-test              # Test mmap package"
	@echo "  make VERBOSE=1 mmap         # Verbose build"
	@echo "  make CLEAN=1 mmap           # Clean and build"
	@echo "  make SKIP_TESTS=1 mmap      # Build without tests"

# Build all packages
build:
	@echo "Building all packages..."
	@$(MAKE) mmap

# Test all packages
test:
	@echo "Testing all packages..."
	@$(MAKE) mmap-test

# Clean all packages
clean:
	@echo "Cleaning all packages..."
	@$(MAKE) mmap-clean

# Build and test all packages
all: build test

# Memory-mapped file package targets
mmap:
	@echo "Building memory-mapped file package..."
	@./scripts/build.sh $(if $(VERBOSE),-v) $(if $(CLEAN),-c) $(if $(SKIP_TESTS),-s) mmap

mmap-test:
	@echo "Testing memory-mapped file package..."
	@./scripts/build.sh $(if $(VERBOSE),-v) -t mmap

mmap-clean:
	@echo "Cleaning memory-mapped file package..."
	@./scripts/build.sh $(if $(VERBOSE),-v) -c -s mmap