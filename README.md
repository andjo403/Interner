interner
============

This library provides an concurrent add only interner that looks only the value with the same hash during interning.
while resize only add of new values is blocked until the resize is done.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
interner = "0.1"
```

## License

Licensed under:
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

