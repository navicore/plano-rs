use plano_api::analytics::query_service_server::{QueryService, QueryServiceServer};
use plano_api::analytics::{QueryRequest, QueryResponse};
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct MyQueryService;

#[tonic::async_trait]
impl QueryService for MyQueryService {
    async fn run_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        println!("Got query: {}", req.sql);
        let reply = QueryResponse { rows: vec![] };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = MyQueryService::default();

    tonic::transport::Server::builder()
        .add_service(QueryServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
