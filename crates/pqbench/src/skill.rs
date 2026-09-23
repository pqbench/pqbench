//! Bundled agent skills (`pqbench skill`).
//!
//! The markdown lives in `skills/` and is compiled into the binary so an
//! agent can load it without the source tree.

use crate::parquet_helpers::Error;

/// One bundled skill.
#[derive(Debug, Clone, Copy)]
pub struct Skill {
    /// Skill name (`parquet-advisor`).
    pub name: &'static str,
    /// When to use it.
    pub description: &'static str,
    /// Documents (`""` is SKILL.md, `"recipes"` is recipes.md).
    pub documents: &'static [SkillDocument],
}

/// One markdown document inside a skill.
#[derive(Debug, Clone, Copy)]
pub struct SkillDocument {
    /// Empty for the main skill body; otherwise the extra name.
    pub name: &'static str,
    /// Full markdown.
    pub markdown: &'static str,
}

const PARQUET_ADVISOR: Skill = Skill {
    name: "parquet-advisor",
    description: "Turns pqbench facts into write-path, table DDL, and compression-level recipes",
    documents: &[
        SkillDocument {
            name: "",
            markdown: include_str!("../../../skills/parquet-advisor/SKILL.md"),
        },
        SkillDocument {
            name: "recipes",
            markdown: include_str!("../../../skills/parquet-advisor/recipes.md"),
        },
    ],
};

/// Skills compiled into this crate.
pub fn skills() -> &'static [Skill] {
    &[PARQUET_ADVISOR]
}

/// Return a skill by name.
///
/// # Errors
/// Fails when the name is unknown.
pub fn skill(name: &str) -> Result<&'static Skill, Error> {
    skills()
        .iter()
        .find(|skill| skill.name == name)
        .ok_or_else(|| {
            Error(format!(
                "unknown skill `{name}`; expected {}",
                skills()
                    .iter()
                    .map(|skill| skill.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

/// Return one markdown document (`""` or `"recipes"`).
///
/// # Errors
/// Fails when the skill or document name is unknown.
pub fn document(name: &str, document: &str) -> Result<&'static str, Error> {
    let skill = skill(name)?;
    skill
        .documents
        .iter()
        .find(|item| item.name == document)
        .map(|item| item.markdown)
        .ok_or_else(|| {
            let extras: Vec<&str> = skill
                .documents
                .iter()
                .map(|item| {
                    if item.name.is_empty() {
                        skill.name
                    } else {
                        item.name
                    }
                })
                .collect();
            Error(format!(
                "unknown skill document `{name} {document}`; expected {}",
                extras.join(", ")
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_advisor_is_bundled() {
        let skill = skill("parquet-advisor").unwrap();
        assert!(skill.description.contains("recipes"));
        let body = document("parquet-advisor", "").unwrap();
        assert!(body.contains("pqbench experiment"));
        assert!(body.contains("Table DDL"));
        let recipes = document("parquet-advisor", "recipes").unwrap();
        assert!(recipes.contains("zstd@3"));
        assert!(recipes.contains("WRITE ORDERED BY"));
    }

    #[test]
    fn unknown_skill_is_an_error() {
        let error = skill("oracle").unwrap_err();
        assert!(error.0.contains("parquet-advisor"), "{error}");
    }
}
