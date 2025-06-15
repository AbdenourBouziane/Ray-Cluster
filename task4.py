import ray
import time
from sklearn.datasets import load_iris
from sklearn.model_selection import KFold
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score
import numpy as np

# Connect to your running Ray cluster
ray.init()

# 1. Load the dataset
iris = load_iris()
X, y = iris.data, iris.target

# Define a remote function to train and evaluate one fold
@ray.remote
def train_and_evaluate_fold(train_indices, test_indices, X_data, y_data):
    """
    This function takes the actual data, trains a model, evaluates it,
    and returns the score.
    """
    # NO ray.get() needed here. Use the data arrays directly.
    X_train, X_test = X_data[train_indices], X_data[test_indices]
    y_train, y_test = y_data[train_indices], y_data[test_indices]

    # Use a standard scikit-learn classifier
    model = SVC()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    score = accuracy_score(y_test, predictions)

    print(f"Fold score: {score:.4f}")
    return score

# Put the large dataset into Ray's object store once to avoid resending it.
X_ref = ray.put(X)
y_ref = ray.put(y)

# 2. Set up K-Fold cross-validation
kf = KFold(n_splits=5, shuffle=True, random_state=42)
print("Starting distributed K-Fold cross-validation...")

# 3. Launch each fold's training and evaluation as a remote task 
start_time = time.time()
score_refs = [
    train_and_evaluate_fold.remote(train_idx, test_idx, X_ref, y_ref)
    for train_idx, test_idx in kf.split(X)
]

# 4. Aggregate the scores using ray.get 
scores = ray.get(score_refs)
end_time = time.time()

print("-" * 20)
print(f"Cross-validation finished in {end_time - start_time:.2f} seconds.")
print(f"Individual fold scores: {scores}")
print(f"Average cross-validation score: {np.mean(scores):.4f}")
print("Distributed ML task complete.")

# Disconnect from Ray
ray.shutdown()