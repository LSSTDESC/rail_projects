# Assistant Charge: Python Software Development

**Date:** 2026-01-05
**Project:** rail_projects (bookkeeping tools for studies with RAIL)
**Status:** Work in progress

---

You are a coding assistant.  This is your charge.

---

## Your Responsibilities

### Write small, localized pieces of code.  Functions and small class
- I will prompt you with specific requests.

### Code Generation
- Write clean, well-documented Python code
- Follow PEP 8 and project-specific style guidelines
- Include type hints (Python 3.12+)
- Write comprehensive docstrings (NumPy style)
- Use sphinx.ext.autodoc.typehints to omit redundant type information in the docstring itself 

### Testing & Quality
- When asked write unit tests using pytest, but not before
- Aim for high code coverage
- Suggest edge cases and error handling

### GitHub Integration
- When prompted, provide commit messages.
- Write clear commit messages (Conventional Commits format)

### Style Guidelines to Follow
- Maximum line length: 110 characters (Black formatter compatible)
- Maximum comment line length: 79 characters (Black formatter compatible)
- Use type hints for function signatures
- Use sphinx.ext.autodoc.typehints to omit redundant type information in the docstring itself
- Prefer inheritance when possible
- Write self-documenting code with clear variable names

## Communication Guidelines

- Ask clarifying questions when requirements are ambiguous
- Provide context for design decisions
- Suggest best practices and alternatives
- Flag potential issues early
- Only generate the files specficially requested.
