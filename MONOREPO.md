# Monorepo Structure

This repository follows a monorepo pattern to house multiple implementations of the Simple Record Store (SRS) specification across different languages and platforms.

## Directory Structure

```
/
├── packages/           # Language/platform-specific implementations
│   ├── mmap/          # Java memory-mapped file implementation
│   ├── mobile/        # Mobile-optimized implementations (planned)
│   └── dart/          # Dart/Flutter implementation (planned)
├── scripts/           # Build and utility scripts
├── SRS-SPEC.md        # Language-agnostic specification
├── Makefile           # Top-level build orchestration
└── README.md          # This file
```

## Package Philosophy

Each package in `packages/` is:
- **Self-contained** - Has its own build system and dependencies
- **Specification-compliant** - Follows the SRS-SPEC.md format
- **Platform-optimized** - Uses the best available storage mechanism for its target platform
- **Independently versioned** - Can be released separately

## Implementation Strategy

### Phase 1: Foundation (Current)
- ✅ Java memory-mapped file implementation (`packages/mmap`)
- ✅ Monorepo structure and build system
- ✅ Language-agnostic specification

### Phase 2: Mobile Support (Planned)
- 🔄 Dart/Flutter implementation (`packages/dart`)
- 🔄 Mobile-optimized Java implementation (`packages/mobile`)
- 🔄 Cross-platform testing

### Phase 3: Platform Expansion (Future)
- 🔄 Additional language implementations
- 🔄 Platform-specific optimizations
- 🔄 Cloud storage backends

## Build System

The monorepo uses a hierarchical build system:

1. **Top-level Makefile** - Orchestrates builds across packages
2. **Package-specific build tools** - Maven for Java, pub for Dart, etc.
3. **POSIX-compliant scripts** - Ensure cross-platform compatibility

## File Format Compatibility

All implementations must:
- Follow the SRS-SPEC.md disk format
- Support the same CRC32 validation
- Maintain write ordering guarantees
- Be crash-safe under power loss

## Development Workflow

1. **Specification changes** - Update SRS-SPEC.md first
2. **Implementation updates** - Update all affected packages
3. **Cross-platform testing** - Verify compatibility across implementations
4. **Documentation** - Update package-specific READMEs

## Future Vision

This monorepo will eventually support:
- **Server-side** - High-performance Java implementation
- **Mobile apps** - Dart/Flutter with platform-specific optimizations
- **Embedded systems** - Minimal C/C++ implementation
- **Cloud storage** - Distributed storage backends
- **Cross-platform sync** - Unified data format across all platforms