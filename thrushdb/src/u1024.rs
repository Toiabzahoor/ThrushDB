// src/types/u1024.rs

use serde::{Deserialize, Serialize};
use std::ops::{BitAnd, BitOr, BitXor};

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct U1024(pub [u64; 16]);

impl U1024 {
    pub const ZERO: Self = Self([0; 16]);

    #[inline(always)]
    pub fn count_ones(&self) -> u32 {
        self.0.iter().map(|w| w.count_ones()).sum()
    }
}

impl BitAnd for U1024 {
    type Output = Self;
    #[inline(always)]
    fn bitand(self, rhs: Self) -> Self::Output {
        let mut out = [0; 16];
        for i in 0..16 { out[i] = self.0[i] & rhs.0[i]; }
        Self(out)
    }
}

impl BitOr for U1024 {
    type Output = Self;
    #[inline(always)]
    fn bitor(self, rhs: Self) -> Self::Output {
        let mut out = [0; 16];
        for i in 0..16 { out[i] = self.0[i] | rhs.0[i]; }
        Self(out)
    }
}

impl BitXor for U1024 {
    type Output = Self;
    #[inline(always)]
    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut out = [0; 16];
        for i in 0..16 { out[i] = self.0[i] ^ rhs.0[i]; }
        Self(out)
    }
}