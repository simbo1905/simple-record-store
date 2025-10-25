# Simple Record Store Monorepo

A multi-language implementation of a crash-safe, random-access record store with multiple storage backends.

## Overview

This monorepo contains multiple implementations of the Simple Record Store (SRS) specification, each optimized for different platforms and use cases:

- **`packages/mmap`** - Full-featured Java implementation using memory-mapped files (not available on mobile)
- **`packages/mobile`** - Mobile-optimized implementations (planned)
- **`packages/dart`** - Dart/Flutter implementation (planned)

## Quick Start

```bash
# Build and test the memory-mapped file implementation
make mmap

# Build all packages
make build

# Run tests for all packages
make test
```

## Package Details

### Memory-Mapped File Implementation (`packages/mmap`)

The flagship implementation featuring:
- Memory-mapped file access for high performance
- Crash-safe operations with CRC32 validation
- Support for large files and concurrent access
- Full Java 21+ compatibility

**Not suitable for mobile platforms** due to memory-mapped file limitations.

See [packages/mmap/README.md](packages/mmap/README.md) for detailed documentation.

## Architecture

All implementations follow the [SRS-SPEC.md](SRS-SPEC.md) specification, ensuring:
- Consistent disk format across all platforms
- Crash-safe operations with proper write ordering
- Language-agnostic data structures
- Interoperable file formats

## Development

Each package is self-contained with its own build system:
- Java packages use Maven
- Dart packages will use `pub` (planned)
- Mobile packages will use platform-specific build tools (planned)

## Contributing

1. Choose the appropriate package for your platform
2. Follow the package-specific development guidelines
3. Ensure all implementations maintain SRS specification compliance
4. Test across all supported platforms

## License

See [LICENSE.txt](LICENSE.txt) for license information.