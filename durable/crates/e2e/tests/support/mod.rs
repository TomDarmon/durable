use std::error::Error;
use substrate::{RootName, RootRegister, ScopedStorage};

pub async fn assert_root_value(
    storage: &ScopedStorage<s3::S3Backend>,
    root: &RootName,
    expected: &[u8],
) -> Result<(), Box<dyn Error>> {
    let state = RootRegister::read(storage, root)
        .await?
        .expect("root should be published");
    assert_eq!(state.value(), expected);
    Ok(())
}
