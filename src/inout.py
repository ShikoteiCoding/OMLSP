# file cannot be named io
import os
import pickle


def save(records: list[dict], txn_id: int, epoch: int, store_location: str) -> None:
    with open(f"{store_location}/{epoch}-{txn_id}.pkl", "wb") as f:
        pickle.dump(records, f, protocol=pickle.HIGHEST_PROTOCOL)


def read_all(store_location: str) -> list[dict] | None:
    files = os.listdir(store_location)
    pickle_files = [f for f in files if f.endswith(".pkl")]
    data = []
    for filename in pickle_files:
        with open(os.path.join(store_location, filename), "rb") as fo:
            data.extend(pickle.load(fo))
    return data


if __name__ == "__main__":
    import time

    store_location = "local-store"
    records = [
        {"url": "https://httpbin.org/get"},
        {"url": "https://httpbin.org/get"},
    ]
    epoch = epoch_time = int(time.time() * 1_000)

    save(records, 0, epoch, store_location)

    data = read_all(store_location)
    print(data)
