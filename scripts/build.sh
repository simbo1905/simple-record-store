#!/bin/sh
#
# POSIX compliant build script for Simple Record Store monorepo
# This script builds and tests the Java memory-mapped file implementation
#

set -e

# Colors for output (POSIX compliant)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
PACKAGE="mmap"
VERBOSE=0
CLEAN=0
TEST=1
SKIP_TESTS=0

# Function to print colored output
print_status() {
    printf "${BLUE}[INFO]${NC} %s\n" "$1"
}

print_success() {
    printf "${GREEN}[SUCCESS]${NC} %s\n" "$1"
}

print_warning() {
    printf "${YELLOW}[WARNING]${NC} %s\n" "$1"
}

print_error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] [PACKAGE]

Build and test the Simple Record Store monorepo packages.

OPTIONS:
    -h, --help      Show this help message
    -v, --verbose   Enable verbose output
    -c, --clean     Clean before building
    -t, --test      Run tests (default)
    -s, --skip-tests Skip running tests
    -p, --package   Specify package to build (default: mmap)

PACKAGES:
    mmap            Memory-mapped file implementation (Java)

EXAMPLES:
    $0                    # Build and test mmap package
    $0 -c mmap           # Clean and build mmap package
    $0 -s                 # Build mmap package without tests
    $0 -v -t              # Verbose build with tests

EOF
}

# Parse command line arguments
while [ $# -gt 0 ]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -c|--clean)
            CLEAN=1
            shift
            ;;
        -t|--test)
            TEST=1
            SKIP_TESTS=0
            shift
            ;;
        -s|--skip-tests)
            SKIP_TESTS=1
            TEST=0
            shift
            ;;
        -p|--package)
            PACKAGE="$2"
            shift 2
            ;;
        mmap)
            PACKAGE="mmap"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate package
case $PACKAGE in
    mmap)
        PACKAGE_DIR="packages/mmap"
        ;;
    *)
        print_error "Unknown package: $PACKAGE"
        print_error "Available packages: mmap"
        exit 1
        ;;
esac

# Check if package directory exists
if [ ! -d "$PACKAGE_DIR" ]; then
    print_error "Package directory not found: $PACKAGE_DIR"
    exit 1
fi

# Check if Maven is available
if ! command -v mvn >/dev/null 2>&1; then
    print_error "Maven is not installed or not in PATH"
    print_error "Please install Maven to build the Java packages"
    exit 1
fi

# Check if Java is available
if ! command -v java >/dev/null 2>&1; then
    print_error "Java is not installed or not in PATH"
    print_error "Please install Java to build the Java packages"
    exit 1
fi

print_status "Building package: $PACKAGE"
print_status "Package directory: $PACKAGE_DIR"

# Change to package directory
cd "$PACKAGE_DIR" || {
    print_error "Failed to change to package directory: $PACKAGE_DIR"
    exit 1
}

# Clean if requested
if [ $CLEAN -eq 1 ]; then
    print_status "Cleaning package..."
    if [ $VERBOSE -eq 1 ]; then
        mvn clean
    else
        mvn clean >/dev/null 2>&1
    fi
    print_success "Clean completed"
fi

# Build the package
print_status "Building package..."
if [ $VERBOSE -eq 1 ]; then
    mvn compile
else
    mvn compile >/dev/null 2>&1
fi
print_success "Compilation completed"

# Run tests if requested
if [ $TEST -eq 1 ] && [ $SKIP_TESTS -eq 0 ]; then
    print_status "Running tests..."
    if [ $VERBOSE -eq 1 ]; then
        mvn test
    else
        mvn test >/dev/null 2>&1
    fi
    print_success "Tests completed"
elif [ $SKIP_TESTS -eq 1 ]; then
    print_warning "Skipping tests as requested"
fi

# Package the JAR
print_status "Creating JAR package..."
if [ $VERBOSE -eq 1 ]; then
    mvn package -DskipTests
else
    mvn package -DskipTests >/dev/null 2>&1
fi
print_success "JAR package created"

print_success "Build completed successfully for package: $PACKAGE"