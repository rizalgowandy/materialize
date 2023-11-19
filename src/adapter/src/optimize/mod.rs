// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer interface to the adapter and coordinator code.
//!
//! The goal of this crate is to abstract optimizer specifics behind a
//! high-level interface that is ready to be consumed by the coordinator code in
//! a future-proof way (that is, the API is taking the upcoming evolution of
//! these components into account).
//!
//! The contents of this crate should have minimal dependencies to the rest of
//! the coordinator code so we can pull them out as a separate crate in the
//! future without too much effort.
//!
//! The main type in this module is a very simple [`Optimize`] trait which
//! allows us to adhere to the following principles:
//!
//! - Implementors of this trait are structs that encapsulate all context
//!   required to optimize a statement of type `T` end-to-end (for example
//!   [`materialized_view::Optimizer`] for `T` = `MaterializedView`).
//! - Each struct implements [`Optimize`] once for each optimization stage. The
//!   `From` type represents the input of the stage and `Self::To` the
//!   associated stage output. This allows to have more than one entrypoints to
//!   a pipeline.
//! - The concrete types used for stage results are opaque structs that are
//!   specific to the pipeline of that statement type.
//!   - We use different structs even if two statement types might have
//!     structurally identical intermediate results. This ensures that client
//!     code cannot first execute some optimization stages for one type and then
//!     some stages for a different type.
//!   - The only way to construct such a struct is by running the [`Optimize`]
//!     stage that produces it. This ensures that client code cannot interfere
//!     with the pipeline.
//!   - In general, the internals of these structs can be accessed only behind a
//!     shared reference. This ensures that client code can look up information
//!     from intermediate stages but cannot modify it.
//!   - Timestamp selection is modeled as a conversion between structs that are
//!     adjacent in the pipeline using a method called `resolve`.
//!   - The struct representing the result of the final stage of the
//!     optimization pipeline can be destructed to access its internals with a
//!     method called `unapply`.
//! - The `Send` trait bounds on the `Self` and `From` types ensure that
//!   [`Optimize`] instances can be passed to different threads (this is
//!   required of off-thread optimization).
//!
//! For details, see the `20230714_optimizer_interface.md` design doc in this
//! repository.

pub mod index;
pub mod materialized_view;
pub mod peek;
pub mod subscribe;
pub mod view;

use mz_catalog::memory::objects::CatalogItem;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain::ExplainConfig;
use mz_repr::GlobalId;
use mz_sql::plan::PlanError;
use mz_sql::session::vars::SystemVars;
use mz_transform::TransformError;

use crate::coord::dataflows::DataflowBuilder;
use crate::AdapterError;

/// A trait that represents an optimization stage.
///
/// The trait is implemented by structs that encapsulate the context needed to
/// run an end-to-end optimization pipeline for a specific statement type
/// (`Index`, `View`, `MaterializedView`, `Subscribe`, `Select`).
///
/// Each implementation represents a concrete optimization stage for a fixed
/// statement type that consumes an input of type `From` and produces output of
/// type `Self::To`.
///
/// The generic lifetime `'ctx` models the lifetime of the optimizer context and
/// can be passed to the optimizer struct and the `Self::To` types.
///
/// The `'s: 'ctx` bound in the `optimize` method call ensures that an optimizer
/// instance can run an optimization stage that produces a `Self::To` with
/// `&'ctx` references.
pub trait Optimize<From>: Send
where
    From: Send,
{
    type To: Send;

    /// Execute the optimization stage, transforming the input plan of type
    /// `From` to an output plan of type `To`.
    fn optimize(&mut self, plan: From) -> Result<Self::To, OptimizerError>;

    /// Execute the optimization stage and panic if an error occurs.
    ///
    /// See [`Optimize::optimize`].
    fn must_optimize(&mut self, expr: From) -> Self::To {
        match self.optimize(expr) {
            Ok(ok) => ok,
            Err(err) => panic!("must_optimize call failed: {err}"),
        }
    }
}

// Feature flags for the optimizer.
#[derive(Clone, Debug)]
pub struct OptimizerConfig {
    /// The mode in which the optimizer runs.
    pub mode: OptimizeMode,
    /// Enable consolidation of unions that happen immediately after negate.
    ///
    /// The refinement happens in the LIR ⇒ LIR phase.
    pub enable_consolidate_after_union_negate: bool,
    /// Enable collecting type information so that rendering can type-specialize
    /// arrangements.
    ///
    /// The collection of type information happens in MIR ⇒ LIR lowering.
    pub enable_specialized_arrangements: bool,
    /// An exclusive upper bound on the number of results we may return from a
    /// Persist fast-path peek. Required by the `create_fast_path_plan` call in
    /// [`peek::Optimizer`].
    pub persist_fast_path_limit: usize,
    /// Enable outer join lowering implemented in #22343.
    pub enable_new_outer_join_lowering: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OptimizeMode {
    /// A mode where the optimized statement is executed.
    Execute,
    /// A mode where the optimized statement is explained.
    Explain,
}

impl From<&SystemVars> for OptimizerConfig {
    fn from(vars: &SystemVars) -> Self {
        Self {
            mode: OptimizeMode::Execute,
            enable_consolidate_after_union_negate: vars.enable_consolidate_after_union_negate(),
            enable_specialized_arrangements: vars.enable_specialized_arrangements(),
            persist_fast_path_limit: vars.persist_fast_path_limit(),
            enable_new_outer_join_lowering: vars.enable_new_outer_join_lowering(),
        }
    }
}

impl From<(&SystemVars, &ExplainConfig)> for OptimizerConfig {
    fn from((vars, explain_config): (&SystemVars, &ExplainConfig)) -> Self {
        // Construct base config from vars.
        let mut config = Self::from(vars);
        // We are calling this constructor from an 'Explain' mode context.
        config.mode = OptimizeMode::Explain;
        // Override feature flags that can be enabled in the EXPLAIN config.
        if let Some(explain_flag) = explain_config.enable_new_outer_join_lowering {
            config.enable_new_outer_join_lowering = explain_flag;
        }
        // Return final result.
        config
    }
}

impl From<&OptimizerConfig> for mz_sql::plan::HirToMirConfig {
    fn from(config: &OptimizerConfig) -> Self {
        Self {
            enable_new_outer_join_lowering: config.enable_new_outer_join_lowering,
        }
    }
}

/// A type for a [`DataflowDescription`] backed by `Mir~` plans. Used internally
/// by the optimizer implementations.
type MirDataflowDescription = DataflowDescription<OptimizedMirRelationExpr>;
/// A type for a [`DataflowDescription`] backed by `Lir~` plans. Used internally
/// by the optimizer implementations.
type LirDataflowDescription = DataflowDescription<Plan>;

/// Error types that can be generated during optimization.
#[derive(Debug, thiserror::Error)]
pub enum OptimizerError {
    // TODO: change dataflows.rs error types and reverse this ownership.
    #[error("{0}")]
    AdapterError(#[from] AdapterError),
    #[error("{0}")]
    PlanError(#[from] PlanError),
    #[error("{0}")]
    TransformError(#[from] TransformError),
    #[error("internal optimizer error: {0}")]
    Internal(String),
}

// TODO: create a dedicated AdapterError::OptimizerError variant.
impl From<OptimizerError> for AdapterError {
    fn from(value: OptimizerError) -> Self {
        match value {
            OptimizerError::AdapterError(err) => err,
            err => AdapterError::Internal(err.to_string()),
        }
    }
}

impl<'a> DataflowBuilder<'a> {
    // Re-optimize the imported view plans using the current optimizer
    // configuration if we are running in `EXPLAIN`.
    pub fn reoptimize_imported_views(
        &self,
        df_desc: &mut MirDataflowDescription,
        config: &OptimizerConfig,
    ) -> Result<(), OptimizerError> {
        if config.mode == OptimizeMode::Explain {
            for desc in df_desc.objects_to_build.iter_mut().rev() {
                if matches!(desc.id, GlobalId::Explain | GlobalId::Transient(_)) {
                    // Skip descriptions that do not reference proper views.
                    continue;
                }
                if let CatalogItem::View(view) = &self.catalog.get_entry(&desc.id).item {
                    let span = tracing::span!(
                        target: "optimizer",
                        tracing::Level::DEBUG,
                        "view",
                        path.segment = desc.id.to_string()
                    );
                    desc.plan = span.in_scope(|| {
                        let mut view_optimizer = view::Optimizer::new(config.clone());
                        view_optimizer.optimize(view.raw_expr.clone())
                    })?;
                }
            }
        }

        Ok(())
    }
}
