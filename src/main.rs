use rfd::FileDialog;
use std::{fs::create_dir, path::PathBuf};
use xdg_home::home_dir;
use iced::{Task, Color, widget::{button, column, text, Column}};

fn get_target_dir_from_user() -> Option<PathBuf> {
    FileDialog::new().pick_folder()
}

#[derive(Clone)]
struct Config {
    conf_dir : PathBuf,
}

struct Init {
    config : Config,
    problem : Result<(),Option<PathBuf>>,
}

struct Work {
    config : Config,
    path : PathBuf,
}

enum State {
    Init(Init),
    Work(Work)
}

#[derive(Debug, Clone, Copy)]
enum Message {
    GetWorkDir,
}

impl State {
    pub fn view(&self) -> Column<Message> {
        match self {
            State::Init(init) => {
                if let Err(path) = &init.problem {
                    column![
                        text(match path {
                            None => "Failed to get target folder from file picker! Try again.".to_owned(),
                            Some(path) => format!("Folder '{:}' does not exist! Try again.", path.to_str().unwrap_or("<directory>")),
                        }).size(50).color(Color::from_rgb(0xff as f32, 0f32, 0f32)),
                        text(format!("Configuration Folder: {:}", init.config.conf_dir.to_str().unwrap_or("<directory>"))).size(50),
                        button("Choose Folder").on_press(Message::GetWorkDir),
                    ]
                } else {
                    column![
                        text(format!("Configuration Folder: {:}", init.config.conf_dir.to_str().unwrap_or("<directory>"))).size(50),
                        button("Choose Folder").on_press(Message::GetWorkDir),
                    ]
                }
            },
            State::Work(work) => {
                column![
                    text(format!("Configuration Folder: {:}", work.config.conf_dir.to_str().unwrap_or("<directory>"))).size(50),
                    text(format!("Folder for deduplication: {:}", work.path.to_str().unwrap_or("<directory>"))).size(50),
                ]
            },
        }
    }

    pub fn update(&mut self, message: Message) {
        match self {
            State::Init(init) => {
                match message {
                    Message::GetWorkDir => {
                        if let Some(path) = get_target_dir_from_user() {
                            if path.exists() {
                                *self = State::Work(Work { config: init.config.clone(), path });
                            } else {
                                init.problem = Err(Some(path));
                            }
                        } else {
                            init.problem = Err(None);
                        }
                    },
                }
            },
            State::Work(_) => {
                todo!()
            }
        }
    }
}

fn main() -> iced::Result {
    let home = home_dir().expect("Couldn't find user's home directory");
    if !home.exists() {
        panic!("User home {:?} doesn't exist", home);
    }
    let conf_dir = home.join(".file-deduplicator");
    if !conf_dir.exists() {
        create_dir(&conf_dir).expect(&format!("Failed to create conf directory: {:?}", conf_dir));
    }
    // Data directory is found.  Now we can create our initial state.  We should also check for and read
    // any previous work.  We need to implement a top level data file that keeps track of all previous work.
    // this way, they can resume previous projects.
    iced::application("File Deduplicator", State::update, State::view).run_with(|| (
        State::Init(Init {
            config: Config { conf_dir },
            problem: Ok(())
        }),
        Task::none()
    ))
}
