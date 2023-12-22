use std::time::{Duration, SystemTime};
use std::thread::sleep;

#[derive(Clone)]
pub struct DebugTimer{
    now: SystemTime,
}

impl DebugTimer{
    pub fn start() -> Self {
        DebugTimer{
            now: SystemTime::now(),
        }
    }

    pub fn elapsed(&self) -> u128 {
        match self.now.elapsed() {
            Ok(elapsed) => {
                return elapsed.as_millis();
            }
            Err(e) => {
                println!("Error: {e:?}");
                return 0;
            }
        }
    }
}







pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
