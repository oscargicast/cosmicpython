import model
import repository


def test_repository_can_save_a_batch():
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)
    repo = repository.FakeRepository()
    repo.add(batch)
    assert repo.list() == [batch]
