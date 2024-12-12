use crate::normalize_string;
use crate::operation_management::OperationError;
use gb_io::seq::{Location, Seq};
use regex::{Error as RegexError, Regex};
use std::fmt;
use std::str::{self, FromStr};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum GenBankError {
    #[error("Feature Location Error: {0}")]
    LocationError(&'static str),
    #[error("Parse Error: {0}")]
    ParseError(String),
    #[error("Operation Error: {0}")]
    OperationError(#[from] OperationError),
    #[error("Regex Error: {0}")]
    Regex(#[from] RegexError),
}

#[derive(Copy, Clone)]
pub enum EditType {
    Deletion,
    Insertion,
    Replacement,
}

impl fmt::Display for EditType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EditType::Deletion => write!(f, "Deletion"),
            EditType::Insertion => write!(f, "Insertion"),
            EditType::Replacement => write!(f, "Replacement"),
        }
    }
}

impl FromStr for EditType {
    type Err = ();

    fn from_str(input: &str) -> Result<EditType, Self::Err> {
        match input {
            "Deletion" => Ok(EditType::Deletion),
            "Insertion" => Ok(EditType::Insertion),
            "Replacement" => Ok(EditType::Replacement),
            _ => Err(()),
        }
    }
}

pub struct GenBankEdit {
    pub start: i64,
    pub end: i64,
    pub old_sequence: String,
    pub new_sequence: String,
    pub edit_type: EditType,
}

pub struct GenBankLocus {
    pub name: String,
    pub molecule_type: Option<String>,
    pub sequence: String,
    pub changes: Vec<GenBankEdit>,
}

impl GenBankLocus {
    pub fn original_sequence(&self) -> String {
        let mut final_sequence = self.sequence.clone();
        let mut offset: i64 = 0;
        for edit in self.changes.iter() {
            let ustart = (edit.start + offset) as usize;
            let uend = (edit.end + offset) as usize;
            match edit.edit_type {
                EditType::Insertion => {
                    final_sequence =
                        format!("{}{}", &final_sequence[..ustart], &final_sequence[uend..]);
                }
                EditType::Deletion | EditType::Replacement => {
                    final_sequence = format!(
                        "{}{}{}",
                        &final_sequence[..ustart],
                        edit.old_sequence,
                        &final_sequence[uend..]
                    );
                }
            }
            offset += edit.old_sequence.len() as i64 - edit.new_sequence.len() as i64;
        }
        final_sequence
    }

    pub fn changes_to_wt(&self) -> Vec<GenBankEdit> {
        let mut wt_changes = vec![];
        let mut offset: i64 = 0;
        for edit in self.changes.iter() {
            let seq_diff = edit.old_sequence.len() as i64 - edit.new_sequence.len() as i64;
            wt_changes.push(GenBankEdit {
                start: edit.start + offset,
                end: edit.end + offset + seq_diff,
                old_sequence: edit.old_sequence.clone(),
                new_sequence: edit.new_sequence.clone(),
                edit_type: edit.edit_type,
            });
            offset += seq_diff;
        }
        wt_changes
    }
}

pub fn process_sequence(seq: Seq) -> Result<GenBankLocus, GenBankError> {
    let final_sequence = if let Ok(sequence) = str::from_utf8(&seq.seq) {
        sequence.to_string()
    } else {
        return Err(GenBankError::ParseError("No sequence present".to_string()));
    };

    let geneious_edit = Regex::new(r"Geneious type: Editing History (?P<edit_type>\w+)")?;
    let mut locus = GenBankLocus {
        name: seq.name.unwrap_or_default(),
        sequence: final_sequence.clone(),
        molecule_type: seq.molecule_type,
        changes: vec![],
    };

    for feature in seq.features.iter() {
        for (key, value) in feature.qualifiers.iter() {
            if key == "note" {
                if let Some(v) = value {
                    let geneious_mod = geneious_edit.captures(v);
                    if let Some(edit) = geneious_mod {
                        let (mut start, mut end) = feature
                            .location
                            .find_bounds()
                            .map_err(|_| GenBankError::LocationError("Ambiguous Bounds"))?;
                        match &edit["edit_type"] {
                            "Insertion" => {
                                // If there is an insertion, it means that the WT is missing
                                // this sequence, so we actually treat it as a deletion
                                locus.changes.push(GenBankEdit {
                                    start,
                                    end,
                                    old_sequence: "".to_string(),
                                    new_sequence: final_sequence[start as usize..end as usize]
                                        .to_string(),
                                    edit_type: EditType::Insertion,
                                });
                            }
                            "Deletion" | "Replacement" => {
                                // If there is a deletion, it means that found sequence is missing
                                // this sequence, so we treat it as an insertion
                                let deleted_seq = normalize_string(
                                    &feature
                                        .qualifiers
                                        .iter()
                                        .filter(|(k, _v)| k == "Original_Bases")
                                        .map(|(_k, v)| v.clone())
                                        .collect::<Option<String>>()
                                        .expect("Deleted sequence is not annotated."),
                                );
                                if matches!(feature.location, Location::Between(_, _)) {
                                    start += 1;
                                    end -= 1;
                                }
                                locus.changes.push(GenBankEdit {
                                    start,
                                    end,
                                    old_sequence: deleted_seq,
                                    new_sequence: final_sequence[start as usize..end as usize]
                                        .to_string(),
                                    edit_type: EditType::from_str(&edit["edit_type"]).unwrap(),
                                });
                            }
                            t => {
                                println!("Unknown edit type {t}.")
                            }
                        }
                    }
                }
            }
        }
    }

    locus.changes.sort_unstable_by(|a, b| a.start.cmp(&b.start));
    Ok(locus)
}
