use std::ops::{Add, Div, Rem};

/// Convert the radix (base) of digits stored in a vector.
pub struct Convert {
  from: u64,
  to: u64,
  ratio: (usize, usize),
}

impl Convert {
  /// Create a new converter with `from` and `to` bases.
  pub fn new(from: u64, to: u64) -> Self {
    let mut ratio = (0, 0);
    if from % to == 0 || to % from == 0 {
      let max_i = 128 / ulog2(to.max(from));
      let mut j = 0;
      let mut k = 0;
      let f = from as u128;
      let t = to as u128;
      for i in 0..max_i {
        let f_j = f.pow(j);
        let t_k = t.pow(k);
        if i > 0 && f_j == t_k {
          ratio.0 = j as usize;
          ratio.1 = k as usize;
          break;
        } else if f_j < t_k || (i == 0 && from > to) {
          j += 1
        } else {
          k += 1
        }
      }
    }
    Convert { from, to, ratio }
  }
  /// Create a new converter but don't test for alignment.
  pub fn new_unaligned(from: u64, to: u64) -> Self {
    Convert {
      from,
      to,
      ratio: (0, 0),
    }
  }
  /// Perform the conversion on `input` which contains digits in base
  /// `self.from`. You should specify the `Output` type so that the target base
  /// (`self.to`) fits. There are no checks to ensure the `Output` type has
  /// room.
  ///
  /// For input and output vectors, the least significant digit is at the
  /// beginning of the array.
  pub fn convert<Input, Output>(&mut self, input: &[Input]) -> Vec<Output>
  where
    Output: Copy
      + Into<u64>
      + From<u8>
      + FromU64
      + Add<Output, Output = Output>
      + Div<Output, Output = Output>
      + Rem<Output, Output = Output>,
    Input: Copy + Into<u64>,
  {
    let len = input.len();
    let cap = len * ulog2(self.from) / ulog2(self.to);
    let mut output: Vec<Output> = Vec::with_capacity(cap);
    let mut base: Vec<Output> = vec![1u8.into()];
    let mut v0: Vec<Output> = vec![];
    let step = self.ratio.0;
    let mut offset = 0;
    for (i, x) in input.iter().enumerate() {
      v0.clear();
      v0.extend(&base);
      self.multiply_scalar_into(&mut v0, (*x).into());
      self.add_into(&mut output, &v0, offset);
      if i + 1 < input.len() {
        self.multiply_scalar_into(&mut base, self.from);
      }
      if step > 0 && i % step == step - 1 {
        base.clear();
        base.push(1u8.into());
        offset += self.ratio.1;
      }
    }
    output
  }
  fn multiply_scalar_into<T>(&self, dst: &mut Vec<T>, x: u64) -> ()
  where
    T: Copy + Into<u64> + FromU64,
  {
    let mut carry = 0u64;
    for i in 0..dst.len() {
      let res = dst[i].into() * x + carry;
      carry = res / self.to;
      dst[i] = FromU64::from(res % (self.to as u64));
    }
    while carry > 0 {
      dst.push(FromU64::from(carry % self.to));
      carry /= self.to;
    }
  }
  fn add_into<T>(&self, dst: &mut Vec<T>, src: &Vec<T>, offset: usize) -> ()
  where
    T: Copy + Into<u64> + FromU64 + Add<T, Output = T> + Div<T, Output = T> + Rem<T, Output = T>,
  {
    let mut carry = 0u64;
    let mut i = 0;
    while dst.len().max(offset) - offset < src.len() {
      dst.push(FromU64::from(0));
    }
    loop {
      let j = i + offset;
      if i < src.len() && j < dst.len() {
        let res = src[i].into() + dst[j].into() + carry;
        carry = res / self.to;
        dst[j] = FromU64::from(res % self.to);
      } else if j < dst.len() {
        let res = dst[j].into() + carry;
        carry = res / self.to;
        dst[j] = FromU64::from(res % self.to);
      } else if i < src.len() {
        let res = src[i].into() + carry;
        carry = res / self.to;
        dst.push(FromU64::from(res % self.to));
      } else if carry > 0 {
        let res = carry;
        carry = res / self.to;
        dst.push(FromU64::from(res % self.to));
      } else {
        break;
      }
      i += 1;
    }
  }
}

fn ulog2(x: u64) -> usize {
  (63 - x.leading_zeros()) as usize
}

// custom trait because TryFrom is difficult:
#[doc(hidden)]
pub trait FromU64 {
  fn from(n: u64) -> Self;
}
impl FromU64 for u8 {
  fn from(n: u64) -> Self {
    n as u8
  }
}
impl FromU64 for u16 {
  fn from(n: u64) -> Self {
    n as u16
  }
}
impl FromU64 for u32 {
  fn from(n: u64) -> Self {
    n as u32
  }
}
impl FromU64 for u64 {
  fn from(n: u64) -> Self {
    n as u64
  }
}
