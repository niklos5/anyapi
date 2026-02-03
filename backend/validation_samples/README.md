## Validation Sample Cases

- `case_clean`: All fields present; mapping should converge quickly.
- `case_missing_fields`: Some targets lack sources; agent should attempt to map or leave nulls.
- `case_type_mismatch`: Values have mixed or unexpected types; agent should still map paths and rely on transforms if needed.
