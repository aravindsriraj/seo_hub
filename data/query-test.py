from data.vector_store import VectorStore
from data.query_planner import QueryPlanner
from data.query_executor import QueryExecutor
from core.config import config

# Initialize
store = VectorStore()
store.load_initial_data("knowledge_base")

# Create executor
executor = QueryExecutor(
    rankings_db=config.RANKINGS_DB_PATH,
    urls_db=config.URLS_DB_PATH,
    aimodels_db=config.AIMODELS_DB_PATH
)

# Test question
question = "How are we performing for data catalog keywords?"
explanation, data, viz = executor.execute(question)

print("Explanation:", explanation)
print("\nData:")
print(data)
