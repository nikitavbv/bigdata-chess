use {
    crate::data::{
        Role,
        File,
        Rank,
        Square,
        CastlingSide,
        Nag,
        PlayerTitle,
    }
};

impl From<pgn_reader::Role> for Role {
    fn from(value: pgn_reader::Role) -> Self {        
        match value {
            pgn_reader::Role::Pawn => Self::Pawn,
            pgn_reader::Role::Knight => Self::Knight,
            pgn_reader::Role::Bishop => Self::Bishop,
            pgn_reader::Role::Rook => Self::Rook,
            pgn_reader::Role::Queen => Self::Queen,
            pgn_reader::Role::King => Self::King,
        }
    }
}

impl From<pgn_reader::File> for File {
    fn from(value: pgn_reader::File) -> Self {
        match value {
            pgn_reader::File::A => File::A,
            pgn_reader::File::B => File::B,
            pgn_reader::File::C => File::C,
            pgn_reader::File::D => File::D,
            pgn_reader::File::E => File::E,
            pgn_reader::File::F => File::F,
            pgn_reader::File::G => File::G,
            pgn_reader::File::H => File::H,
        }
    }
}

impl From<pgn_reader::Rank> for Rank {
    fn from(value: pgn_reader::Rank) -> Self {
        match value {
            pgn_reader::Rank::First => Self::First,
            pgn_reader::Rank::Second => Self::Second,
            pgn_reader::Rank::Third => Self::Third,
            pgn_reader::Rank::Fourth => Self::Fourth,
            pgn_reader::Rank::Fifth => Self::Fifth,
            pgn_reader::Rank::Sixth => Self::Sixth,
            pgn_reader::Rank::Seventh => Self::Seventh,
            pgn_reader::Rank::Eighth => Self::Eighth,
        }
    }
}

impl From<pgn_reader::Square> for Square {
    fn from(value: pgn_reader::Square) -> Self {
        Self {
            file: value.file().into(),
            rank: value.rank().into(),
        }
    }
}

impl From<pgn_reader::CastlingSide> for CastlingSide {
    fn from(value: pgn_reader::CastlingSide) -> Self {
        match value {
            pgn_reader::CastlingSide::KingSide => Self::KingSide,
            pgn_reader::CastlingSide::QueenSide => Self::QueenSide,
        }
    }
}

impl From<pgn_reader::Nag> for Nag {
    fn from(value: pgn_reader::Nag) -> Self {
        match value {
            pgn_reader::Nag::GOOD_MOVE => Self::GoodMove,
            pgn_reader::Nag ::MISTAKE => Self::Mistake,
            pgn_reader::Nag::BRILLIANT_MOVE => Self::BrilliantMove,
            pgn_reader::Nag::BLUNDER => Self::Blunder,
            pgn_reader::Nag::SPECULATIVE_MOVE => Self::SpeculativeMove,
            pgn_reader::Nag::DUBIOUS_MOVE => Self::DubiousMove,
            other => panic!("Unexpected nag: {}", other),
        }
    }
}

impl TryFrom<&str> for PlayerTitle {
    type Error = String;
    
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "FM" => Self::FideMaster,
            "IM" => Self::InternationalMaster,
            "NM" => Self::NationalMaster,
            "BOT" => Self::Bot,
            "CM" => Self::CandidateMaster,
            "GM" => Self::Grandmaster,
            "WIM" => Self::WomanInternationalMaster,
            "WFM" => Self::WomanFideMaster,
            "LM" => Self::LichessMaster,
            "WGM" => Self::WomanGrandmaster,
            "WCM" => Self::WomanCandidateMaster,
            other => return Err(format!("Unexpected player title: {}", other)),
        })
    }
}