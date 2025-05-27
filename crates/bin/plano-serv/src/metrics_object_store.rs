///
/// experimental metrics object store wrapper - probably not necessary as metrics are collected
/// elsewhere
///
use bytes::Bytes;
use futures::stream::BoxStream;
use metrics::{counter, Counter};
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};
use std::{fmt::Display, ops::Range, sync::Arc};

use std::sync::LazyLock;
static PUT_OPTS: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_put_opts_total"));
static GET_OPTS: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_get_opts_total"));
static PUT_MULTIPART_OPTS: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_put_multipart_opts_total"));
static DELETE: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_delete_total"));
static LIST: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_list_total"));
static LIST_WITH_DELIMITER: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_list_with_delimiter_total"));
static COPY: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_copy_total"));
static COPY_IF_NOT_EXISTS: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_copy_if_not_exists_total"));
static GET_RANGE: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_get_range_total"));

#[derive(Debug)]
pub struct MetricsObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl Display for MetricsObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricsObjectStore wrapping {}", self.inner)
    }
}

impl MetricsObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl ObjectStore for MetricsObjectStore {
    #[doc = " Save the provided `payload` to `location` with the given options"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<PutResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        PUT_OPTS.increment(1);
        self.inner.put_opts(location, payload, opts)
    }

    #[doc = " Perform a multipart upload with options"]
    #[doc = ""]
    #[doc = " Client should prefer [`ObjectStore::put`] for small payloads, as streaming uploads"]
    #[doc = " typically require multiple separate requests. See [`MultipartUpload`] for more information"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOpts,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<Box<dyn MultipartUpload>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        PUT_MULTIPART_OPTS.increment(1);
        self.inner.put_multipart_opts(location, opts)
    }

    #[doc = " Perform a get request with options"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<GetResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        GET_OPTS.increment(1);
        self.inner.get_opts(location, options)
    }

    #[doc = " Delete the object at the specified location."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        DELETE.increment(1);
        self.inner.delete(location)
    }

    #[doc = " List all the objects with the given prefix."]
    #[doc = ""]
    #[doc = " Prefixes are evaluated on a path segment basis, i.e. `foo/bar` is a prefix of `foo/bar/x` but not of"]
    #[doc = " `foo/bar_baz/x`. List is recursive, i.e. `foo/bar/more/x` will be included."]
    #[doc = ""]
    #[doc = " Note: the order of returned [`ObjectMeta`] is not guaranteed"]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        LIST.increment(1);
        self.inner.list(prefix)
    }

    #[doc = " List objects with the given prefix and an implementation specific"]
    #[doc = " delimiter. Returns common prefixes (directories) in addition to object"]
    #[doc = " metadata."]
    #[doc = ""]
    #[doc = " Prefixes are evaluated on a path segment basis, i.e. `foo/bar` is a prefix of `foo/bar/x` but not of"]
    #[doc = " `foo/bar_baz/x`. List is not recursive, i.e. `foo/bar/more/x` will not be included."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<ListResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        LIST_WITH_DELIMITER.increment(1);
        self.inner.list_with_delimiter(prefix)
    }

    #[doc = " Copy an object from one path to another in the same object store."]
    #[doc = ""]
    #[doc = " If there exists an object at the destination, it will be overwritten."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        COPY.increment(1);
        self.inner.copy(from, to)
    }

    #[doc = " Copy an object from one path to another, only if destination is empty."]
    #[doc = ""]
    #[doc = " Will return an error if the destination already has an object."]
    #[doc = ""]
    #[doc = " Performs an atomic operation if the underlying object storage supports it."]
    #[doc = " If atomic operations are not supported by the underlying object storage (like S3)"]
    #[doc = " it will return an error."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        COPY_IF_NOT_EXISTS.increment(1);
        self.inner.copy_if_not_exists(from, to)
    }
    ///////////// default methods /////////////

    #[doc = " Return the bytes that are stored at the specified location"]
    #[doc = " in the given byte range."]
    #[doc = ""]
    #[doc = " See [`GetRange::Bounded`] for more details on how `range` gets interpreted"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::async_yields_async,
        clippy::diverging_sub_expression,
        clippy::let_unit_value,
        clippy::needless_arbitrary_self_type,
        clippy::no_effect_underscore_binding,
        clippy::shadow_same,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds,
        clippy::used_underscore_binding
    )]
    fn get_range<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        range: Range<u64>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<Bytes>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        GET_RANGE.increment(1);
        self.inner.get_range(location, range)
    }
}
