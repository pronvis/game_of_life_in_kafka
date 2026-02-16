pub mod life_cell;

pub struct LifeCell {
    pub x: u16,
    pub y: u16,
}

impl LifeCell {
    pub fn new(x: u16, y: u16) -> Self {
        Self { x, y }
    }
}

pub trait ToTopic {
    fn to_topic(&self) -> String;
}

impl ToTopic for LifeCell {
    fn to_topic(&self) -> String {
        (self.x as i32, self.y as i32).to_topic()
    }
}

impl ToTopic for (i32, i32) {
    fn to_topic(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}
