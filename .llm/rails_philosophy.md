# Rails Philosophy for hygge

## Core Principles

### 1. Optimize for Programmer Happiness
- APIs should feel natural and comfortable
- Code should make developers smile
- Prioritize user experience over technical perfection
- **hygge application**: Make data movement feel natural, not forced

### 2. Convention over Configuration
- Smart defaults eliminate repetitive decisions
- "You're not a beautiful and unique snowflake" - embrace conventions
- Lower barriers for beginners while empowering experts
- **hygge application**: `home: path` instead of complex configs

### 3. The Menu is Omakase
- Curated stack decisions, not endless choice paralysis
- Safety in numbers - shared experience and community
- Substitutions possible but not required
- **hygge application**: Polars + PyArrow + Pydantic by default

### 4. No One Paradigm
- Use the right tool for each job, not one-size-fits-all
- Mix object-oriented, functional, and procedural approaches
- Embrace pragmatism over purity
- **hygge application**: Async patterns for I/O, classes for models, functions for utilities

### 5. Exalt Beautiful Code
- Aesthetically pleasing code is valuable
- Intersection of native idioms and domain-specific language
- Readable, simple, powerful declarations
- **hygge application**: `flow.run()` feels natural, not `FlowExecutor.execute(flow_instance)`

### 6. Provide Sharp Knives
- Trust programmers with powerful tools
- Assume they want to become better, not worse
- Education over restriction
- **hygge application**: Allow custom Home/Store implementations

### 7. Value Integrated Systems
- Majestic monoliths over premature microservices
- Empowering individuals to build complete systems
- Reduce boundaries and abstractions
- **hygge application**: Single framework for data movement, not scattered tools

### 8. Progress over Stability
- Evolution keeps frameworks relevant
- Occasional breaking changes for long-term benefit
- Push community forward, don't get stuck in the past
- **hygge application**: Keep improving the config system and error handling

### 9. Push up a Big Tent
- Welcome disagreement and diversity of thought
- No litmus tests for community membership
- Fresh ideas from new contributors
- **hygge application**: Support different data sources and use cases

## Key Takeaways for hygge Development

- **Comfort over complexity**: APIs should feel natural
- **Smart defaults**: Convention over configuration
- **Trust developers**: Provide powerful tools with good defaults
- **Integrated approach**: One framework, not scattered tools
- **Beautiful code**: Aesthetic value matters
- **Progress mindset**: Keep evolving and improving
