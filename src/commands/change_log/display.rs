use gen::models::change_log::{ChangeLogSummary, ChangeSet};
use gen::models::Collection;
use std::fmt;

pub struct ChangeSummaryDisplay(pub ChangeLogSummary);
impl fmt::Display for ChangeSummaryDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!(
            "{id}
   Parental Sequence: {left_sequence:>20} {impacted_sequence} {right_sequence:<20}
   Modification {blank:26} {updated_sequence}
            ",
            id = self.0.id,
            blank = " ",
            left_sequence = self.0.parent_left,
            impacted_sequence = self.0.parent_impacted,
            right_sequence = self.0.parent_left,
            updated_sequence = self.0.new_sequence,
        ))
    }
}

pub struct ChangeSetDisplay(pub ChangeSet);
impl fmt::Display for ChangeSetDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!(
            "{id}: {message}",
            id = self.0.id.unwrap(),
            message = self.0.message,
        ))
    }
}

pub struct CollectionDisplay(pub Collection);

impl fmt::Display for CollectionDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("{name}", name = self.0.name,))
    }
}
