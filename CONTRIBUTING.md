# Contributing to CANOE transportation

Thank you for your interest in contributing to CANOE transportation!  
This guide explains how to propose changes, report issues, and improve the project.

---

## Getting Started
- Check existing issues and pull requests before creating new ones.  
- For substantial changes, open a design proposal first (label: `proposal`).  
- Make sure you can run the Quickstart in the README.  


---

## Contribution Workflow
1. Fork the repository and create a branch:  
   ```bash
   git checkout -b feature/<short-desc>
   ```
2. Make changes and run
3. Update documentation and `CHANGELOG.md` (Unreleased section) if there are user-visible changes.  
4. Open a pull request to `main`, explaining:  
   - Why the change is needed  
   - What was changed  
   - How it was tested  
5. Address review feedback; we use **Squash & Merge**.  

---

## Style & Quality
- **Python:** use ruff + black; type hints encouraged (mypy where enabled).  
- **SQL:** use sqlfluff (ANSI or project dialect); write parameterized queries.  
- **Docs:** keep README Quickstart accurate; add examples/tutorials if behavior changes.  
- **Tests:** add unit/integration tests for new features; avoid flaky tests.  

---

## Security & Data
- Never commit secrets — use environment variables.  
- Avoid unsafe deserialization (`pickle`, unsafe `yaml.load`).  
- Report vulnerabilities privately via canoe-support@googlegroups.com.  

---

## Contribution Licensing
By submitting a pull request, you agree that:  
- Your contributions are licensed under the **GPL-3.0 License**, and  
- The CANOE project maintainers may, at their discretion, also relicense your contributions under the **MIT License** or another OSI-approved open-source license.  

This ensures that the project remains compatible with its dependencies (currently GPL-3.0) while retaining the option to move to MIT in the future.  

**To document your agreement, please Sign your commits with the `-s` flag (adds a `Signed-off-by` line)**

---

## Community
- **Questions:** open an Issue with the `question` label, or use Discussions if enabled.  


---


### Reviewer checklist
- [ ] All commits have DCO sign-off (`Signed-off-by:` trailer)  ← required
- [ ] CI checks pass (lint/tests/docs)
- [ ] Conversations resolved; no TODOs left
- [ ] Code Owners approval present (if paths match)
- [ ] No secrets in diff; schema/data changes documented
