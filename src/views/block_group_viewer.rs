use crate::views::block_layout::ScaledLayout;
use core::panic;
use std::u32;
use log::info;
use ratatui::{
    layout::Rect,
    widgets::{Block, Borders},
    widgets::canvas::{Canvas, Line},
    style::{Style, Color, Stylize, Modifier},
    text::Span,
};


/// Holds data for the self.
pub struct Viewer {
    pub layout: ScaledLayout, // Coordinates and labels
    pub scroll: ScrollState,
    pub plot_area: Rect, // Usable area for the plot
    pub plot_parameters: PlotParameters,
}

impl Viewer {
    /// Check if a block is visible in the viewport.
    pub fn is_block_visible(&self, block_id: u32) -> bool {
        if let Some((_, x, y)) = self.layout.labels.get(&block_id) {
            let viewport_left = self.scroll.offset_x;
            let viewport_right = self.scroll.offset_x + self.plot_area.width as i32;
            let viewport_top = self.scroll.offset_y + self.plot_area.height as i32; // z-axis is upside down
            let viewport_bottom = self.scroll.offset_y;

            return (*y as i32) >= viewport_bottom 
                && (*y as i32) < viewport_top 
                && (*x as i32) >= viewport_left 
                && (*x as i32) < viewport_right;
        }
        false
    }

    /// Unselect the currently selected block if it's not visible in the viewport.
    pub fn unselect_if_not_visible(&mut self) {
        if let Some(selected_block) = self.scroll.selected_block {
            if !self.is_block_visible(selected_block) {
                self.scroll.selected_block = None;
            }
        }
    }

    /// Center the viewport on a specific block
    /// - If the block is not present in the layout, panic.
    pub fn center_on_block(&mut self, block_id: u32) {
        if let Some((_, x, y)) = self.layout.labels.get(&block_id) {
            self.scroll.offset_x = *x as i32 - (self.plot_area.width as f64 / 2.0).round() as i32;
            self.scroll.offset_y = *y as i32 - (self.plot_area.height as f64 / 2.0).round() as i32;
        } else {
            panic!("Block ID {} not found in layout", block_id);
        }
    }

    /// Draw and render blocks and lines to a canvas through a scrollable window.
    pub fn paint_canvas(&mut self, frame: &mut ratatui::Frame, area: Rect) {
        // Set up the coordinate systems for the window and the canvas,
        // we need to keep a 1:1 mapping between coordinates to avoid glitches.

        // Terminal window coordinates
        let block = Block::default().borders(Borders::ALL)
                                    .style(Style::new().white().on_black())
                                    .title("graph view");
        self.plot_area = block.inner(area);

        // Data coordinates (the top-left corner of our view is (offset_x, offset_y))
        let viewport_left = self.scroll.offset_x;
        let viewport_right = self.scroll.offset_x + self.plot_area.width as i32;
        let viewport_top = self.scroll.offset_y + self.plot_area.height as i32; // z-axis is upside down
        let viewport_bottom = self.scroll.offset_y;

        // Create the canvas
        let canvas = Canvas::default()
            .block(block)
            // Adjust the x_bounds and y_bounds by the scroll offsets.
            .x_bounds([viewport_left as f64, viewport_right as f64])
            .y_bounds([viewport_bottom as f64, viewport_top as f64])
            .paint(|ctx| {
                // Draw the lines described in the processed layout
                for ((x1, y1), (x2, y2)) in &self.layout.lines {
                    // Clip the line to the visible area, skip if it's not visible itself
                    if let Some(((x1c, y1c), (x2c, y2c))) = clip_line((*x1, *y1), (*x2, *y2), 
                        (viewport_left as f64, viewport_bottom as f64), 
                        (viewport_right as f64, viewport_top as f64)) {
                        ctx.draw(&Line {
                            x1: x1c,
                            y1: y1c,
                            x2: x2c,
                            y2: y2c,
                            color: Color::Gray,
                        });
                    }
                }
                // Print the labels
                for (block_id, (label, x, y)) in &self.layout.labels {
                    // Skip labels that are not in the visible area (vertical)
                    if (*y as i32) < viewport_bottom || (*y as i32) >= viewport_top {
                        continue;
                    }
                    // Clip labels that are potentially in the window (horizontal)
                    let clipped_label = clip_label(label, *x as isize, 
                        (viewport_left + 1) as isize, self.plot_area.width as usize);
                    if !clipped_label.is_empty() {
                        // Style the label depending on whether it's selected
                        let style = if Some(*block_id) == self.scroll.selected_block {
                            Style::default().fg(Color::LightGreen).add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::White)
                        };
                        ctx.print((*x as isize).max(viewport_left as isize) as f64, *y as f64, 
                            Span::styled(clipped_label, style));
                    }
                }
            });
        frame.render_widget(canvas, area);   
    }

    /// Cycle through visible blocks in the viewport
    /// - If no block is selected, it selects the first block in the viewport.
    /// - If a block is selected, it selects the next block in the viewport.
    /// - If the last block is selected, it selects the first block in the viewport.
    /// - If no blocks are in the viewport, it does nothing.
    /// - The direction can be reversed by setting `reverse` to true.
    /// - The selected block is stored in the viewer state, under scrollstate.
    pub fn cycle_blocks(&mut self, reverse: bool) {
        let mut blocks_in_viewport: Vec<u32> = self.layout.labels.keys()
            .filter(|&&block_id| self.is_block_visible(block_id))
            .cloned()
            .collect();

        blocks_in_viewport.sort_by(|a, b| {
            let (_, ax, ay) = self.layout.labels.get(a).unwrap();
            let (_, bx, by) = self.layout.labels.get(b).unwrap();
            by.cmp(ay).then_with(|| ax.cmp(bx))
        });

        if blocks_in_viewport.is_empty() {
            info!("No blocks in the viewport");
            return;
        }
        if self.scroll.selected_block.is_none() {
            if !reverse {
                self.scroll.selected_block = Some(blocks_in_viewport[0]);
                info!("Selected block: {}", blocks_in_viewport[0]);
            } else {
                self.scroll.selected_block = Some(blocks_in_viewport[blocks_in_viewport.len() - 1]);
                info!("Selected block: {}", blocks_in_viewport[blocks_in_viewport.len() - 1]);
            }
        } else {
            let selected_block = self.scroll.selected_block.unwrap();
            let next_block = if let Some(index) = blocks_in_viewport.iter().position(|&id| id == selected_block) {
                let next_index = if reverse {
                    if index == 0 {
                        blocks_in_viewport.len() - 1
                    } else {
                        index - 1
                    }
                } else {
                    if index == blocks_in_viewport.len() - 1 {
                        0
                    } else {
                        index + 1
                    }
                };
                blocks_in_viewport[next_index]
            } else {
                blocks_in_viewport[0]
            };
            self.scroll.selected_block = Some(next_block);
            info!("Selected block: {}", next_block);
        }
    }
}

/// Holds current scrolling offset and a zoom factors for data units per terminal cell.
/// - `block_len` = how much of the sequence to show in each block label.
pub struct ScrollState {
    pub offset_x: i32,
    pub offset_y: i32,
    pub selected_block: Option<u32>,
}

/// Holds parameters that don't change when you scroll.
/// - `label_width` = how many characters to show at most in each block label.
/// - `scale` = data units per 1 terminal cell.  
///   - If `scale` = 1.0, each cell is 1 data unit.  
///   - If `scale` = 2.0, each cell is 2 data units (you see *more* data).  
///   - If `scale` = 0.5, each cell is 0.5 data units (you see *less* data, zoomed in).
/// - `aspect_ratio` = width / height of a terminal cell in data units.
pub struct PlotParameters {
    pub label_width: u32, 
    pub scale: u32, 
    pub aspect_ratio: f32, 
}




/// Clips a string to a specific window, indicating that it has been clipped.
/// - If the string is empty, it returns an empty string.
/// - If the string is shorter than the window, it returns the string.
/// - If the string is longer than the window, it clips the string and replaces the last character with a period.
/// - If the string is not within the window at all, it returns an empty string.
pub fn clip_label(label: &str, label_start: isize, window_start: isize, window_width: usize) -> String {
    if label.is_empty() {
        return "".to_string();
    }

    let label_end = label_start + label.chars().count() as isize - 1;
    let window_end = window_start + window_width as isize - 1;
    if label_end < window_start || label_start > window_end {
        return "".to_string();
    }
 
    if label_start >= window_start && label_end <= window_end {
        return label.to_string();
    }

    let mut clipped = label.to_string();

    // Process the right side first so we don't lose alignment:
    if label_end > window_end {
        let delta_right = label_end - window_end;
        // Make sure we don't try to cut in the middle of a multibyte character
        let character_cutoff = (label.chars().count() as isize - delta_right - 1) as usize;
        let byte_cutoff = label.char_indices().nth(character_cutoff).map(|(i, _)| i).unwrap_or(label.len());
        clipped.replace_range(byte_cutoff.., "…");
    }
    if window_start > label_start {
        let delta_left = window_start - label_start;
        let character_cutoff = delta_left as usize + 1;
        let byte_cutoff = label.char_indices().nth(character_cutoff).map(|(i, _)| i).unwrap_or(label.len());
        clipped.replace_range(..byte_cutoff, "…");
    }

    clipped
}

/// Clip a line given as two points to a specific window, also given by two points
/// - If the line is completely outside the window, it returns None.
/// - If the line is completely inside the window, it returns the original line.
/// - If the line is partially inside the window, it clips the line to the window.
/// 
/// This may be made more efficient through bitwise comparisons (see Cohen-Sutherland line clipping algorithm)
pub fn clip_line(
    (x1, y1): (f64, f64), // Line start
    (x2, y2): (f64, f64), // Line end
    (wx1, wy1): (f64, f64), // Window top left
    (wx2, wy2): (f64, f64), // Window bottom right
) -> Option<((f64, f64), (f64, f64))> {
    let mut t0 = 0.0;
    let mut t1 = 1.0;
    let dx = x2 - x1;
    let dy = y2 - y1;

    let clip = |p: f64, q: f64, t0: &mut f64, t1: &mut f64| -> bool {
        if p == 0.0 {
            return q >= 0.0;
        }
        let r = q / p;
        if p < 0.0 {
            if r > *t1 {
                return false;
            }
            if r > *t0 {
                *t0 = r;
            }
        } else {
            if r < *t0 {
                return false;
            }
            if r < *t1 {
                *t1 = r;
            }
        }
        true
    };

    if clip(-dx, x1 - wx1, &mut t0, &mut t1)
        && clip(dx, wx2 - x1, &mut t0, &mut t1)
        && clip(-dy, y1 - wy1, &mut t0, &mut t1)
        && clip(dy, wy2 - y1, &mut t0, &mut t1)
    {
        let nx1 = x1 + t0 * dx;
        let ny1 = y1 + t0 * dy;
        let nx2 = x1 + t1 * dx;
        let ny2 = y1 + t1 * dy;
        Some(((nx1, ny1), (nx2, ny2)))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    fn test_clip_line_helper(
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        wx1: f64,
        wy1: f64,
        wx2: f64,
        wy2: f64,
        expected: Option<((f64, f64), (f64, f64))>,
    ) {
        let clipped = clip_line((x1, y1), (x2, y2), (wx1, wy1), (wx2, wy2));
        assert_eq!(clipped, expected);
    }

    #[test]
    fn test_clip_line_outside() {
        test_clip_line_helper(0.0, 0.0,
                              1.0, 1.0, 
                              2.0, 2.0, 
                              3.0, 3.0, 
                              None);
    }

    #[test  ]
    fn test_clip_line_inside() {
        test_clip_line_helper(0.0, 0.0,
                              1.0, 1.0, 
                              -1.0, -1.0, 
                              1.5, 1.5, 
                              Some(((0.0, 0.0), (1.0, 1.0))));
    }

    #[test  ]
    fn test_clip_line_partial() {
        test_clip_line_helper(0.0, 0.0,
                              1.0, 1.0, 
                              0.5, 0.5, 
                              1.5, 1.5, 
                              Some(((0.5, 0.5), (1.0, 1.0))));
    }

    #[test]
    fn test_clip_label_multibyte_character() {
        // str.len() counts bytes, not characters (this is a bug in the original implementation)
        let clipped = clip_label("●", 160, -6, 168);
        assert_eq!(clipped, "●");
    }

    #[test]
    fn test_clip_label_negative_offset() {
        //  -2   0 1 2 3 4 5 6 7 8 9 
        //  [        A B C]D E  
        let clipped = clip_label("ABCDE", 2, -2, 7);
        assert_eq!(clipped, "AB…");
    }

    #[test]
    fn test_clip_label_internal() {
        // 0 1 2 3 4 5 6 7 8 9 
        //  [  A B C D E  ] 
        let clipped = clip_label("ABCDE", 2, 1, 7);
        assert_eq!(clipped, "ABCDE");
    }

    #[test]
    fn test_clip_label_external() {
        // 0 1 2 3 4 5 6 7 8 9 
        //     A B  [     ] 
        let clipped = clip_label("AB", 2, 5, 3);
        assert_eq!(clipped, "");
    }

    #[test]
    fn test_clip_label_left() {
        // 0 1 2 3 4 5 6 7 8 9 
        //     A B[C D E F G H] 
        let clipped = clip_label("ABCDEFGH", 2, 4, 10);
        assert_eq!(clipped, "…DEFGH");
    }

    #[test]
    fn test_clip_label_right() {
        // 0 1 2 3 4 5 6 7 8 9 
        //[    A B C D E]F G H
        let clipped = clip_label("ABCDEFGH", 2, 0, 7);
        assert_eq!(clipped, "ABCD…");
    }

    #[test]
    fn test_clip_label_both() {
        // 0 1 2 3 4 5 6 7 8 9 
        //     A B[C D E]F G H
        let clipped = clip_label("ABCDEFGH", 2, 4, 3);
        assert_eq!(clipped, "…D…");
    }

}