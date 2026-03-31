///
/// experimental metrics object store wrapper - probably not necessary as metrics are collected
/// elsewhere
///
use futures::stream::BoxStream;
use metrics::{counter, Counter};
use object_store::PutMultipartOptions;
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutOptions, PutPayload, PutResult, Result,
};
use std::pin::Pin;
use std::{fmt::Display, sync::Arc};

use std::sync::LazyLock;
static PUT_OPTS: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_put_opts_total"));
static GET_OPTS: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_get_opts_total"));
static PUT_MULTIPART_OPTS: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_put_multipart_opts_total"));
static DELETE_STREAM: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_delete_stream_total"));
static LIST: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_list_total"));
static LIST_WITH_DELIMITER: LazyLock<Counter> =
    LazyLock::new(|| counter!("plano_store_list_with_delimiter_total"));
static COPY_OPTS: LazyLock<Counter> = LazyLock::new(|| counter!("plano_store_copy_opts_total"));

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
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Pin<
        Box<dyn core::future::Future<Output = Result<PutResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        PUT_OPTS.increment(1);
        self.inner.put_opts(location, payload, opts)
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOptions,
    ) -> Pin<
        Box<
            dyn core::future::Future<Output = Result<Box<dyn MultipartUpload>>>
                + Send
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

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> Pin<
        Box<dyn core::future::Future<Output = Result<GetResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        GET_OPTS.increment(1);
        self.inner.get_opts(location, options)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        DELETE_STREAM.increment(1);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        LIST.increment(1);
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> Pin<
        Box<dyn core::future::Future<Output = Result<ListResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        LIST_WITH_DELIMITER.increment(1);
        self.inner.list_with_delimiter(prefix)
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
        opts: CopyOptions,
    ) -> Pin<
        Box<dyn core::future::Future<Output = Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        COPY_OPTS.increment(1);
        self.inner.copy_opts(from, to, opts)
    }
}
