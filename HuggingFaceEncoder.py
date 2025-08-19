from sentence_transformers import SentenceTransformer

class HuggingFaceEncoder:

    def __init__(self, model_name='sentence-transformers/all-MiniLM-L6-v2'):

        print(f"Loading embedding model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        print(f"Model loaded. Embedding dimension: {self.embedding_dim}")
        print("-" * 50)

    def encode(self, sentences):
        print(f"Encoding {len(sentences)} sentences...")
        embeddings = self.model.encode(sentences, convert_to_numpy=True, show_progress_bar=True)
        print("Encoding complete.")
        return embeddings
