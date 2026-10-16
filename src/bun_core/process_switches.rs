//! The switches of this process (`code_generation`, `js_macros`) as flags for a `bun` that it starts to run a part of the same command.

use core::ffi::CStr;

use crate::{
    CODE_GENERATION_FROM_STRINGS_DISALLOWED_BY_BUILD, CodeGenerationFromStrings,
    code_generation_from_strings, js_macros_disabled,
};

pub fn flags_for_child() -> impl Iterator<Item = &'static CStr> {
    // A build with the level as a constant gives it to the child without a flag, which nobody typed.
    let code_generation = if CODE_GENERATION_FROM_STRINGS_DISALLOWED_BY_BUILD {
        None
    } else {
        match code_generation_from_strings() {
            CodeGenerationFromStrings::Allowed => None,
            CodeGenerationFromStrings::DisallowedLikeNode => {
                Some(c"--disallow-code-generation-from-strings")
            }
            CodeGenerationFromStrings::Strict => {
                Some(c"--disallow-code-generation-from-strings=strict")
            }
        }
    };
    let macros = js_macros_disabled().then_some(c"--no-macros");
    code_generation.into_iter().chain(macros)
}
