interner
============

This library provides an concurrent insert only interner.
Inserts is only locking one slot and store part of the hash in the look to let other inserts with eough diffrent hash to not block on the looked slot.
During resize insert is still possible and if the value was already interned only possibly extra lookup in newer interners is done.
During resize all work with moving is done by threads that needs to insert a new value in the interner.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
interner = { git = "https://github.com/andjo403/Interner.git" }
```

## License

Licensed under:
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

