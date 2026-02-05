# Contributing

Thanks for your interest in contributing to Cross-Region Model Serving with Delta Sharing!

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-username>/cross-region-model-serving-dab.git
   cd cross-region-model-serving-dab
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install pytest ruff
   ```
4. Create a branch:
   ```bash
   git checkout -b feat/your-feature
   ```

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Linting

```bash
ruff check src/ tests/
```

### Building the Wheel

```bash
python setup.py bdist_wheel
```

### Validating the Bundle

```bash
databricks bundle validate -t dev
```

## Pull Request Process

1. Ensure all tests pass and linting is clean
2. Update `CHANGELOG.md` with your changes under an `[Unreleased]` section
3. Use conventional commit messages: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`
4. Open a PR against `main` with a clear description of what and why
5. One approval required before merge

## Reporting Issues

- Use GitHub Issues with a clear title and reproduction steps
- Include your Databricks Runtime version, CLI version, and cloud provider
- For security issues, see [SECURITY.md](SECURITY.md)

## Code Style

- Follow PEP 8
- Type hints on public function signatures
- Docstrings on public functions (explain "why" not "what")
- Error messages should include: what failed, which resource, and how to fix it
