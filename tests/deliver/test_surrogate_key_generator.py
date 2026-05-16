"""Tests for SurrogateKeyGenerator (Kimball Subsystem #10)."""

import threading

import pytest

from src.deliver.surrogate_key_generator import SKGeneratorError, SurrogateKeyGenerator


class TestNextSk:
    def test_starts_at_one(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        assert gen.next_sk("dim_x") == 1

    def test_increments_monotonically(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        results = [gen.next_sk("dim_x") for _ in range(5)]
        assert results == [1, 2, 3, 4, 5]

    def test_separate_sequences_per_dim(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.next_sk("dim_a")
        gen.next_sk("dim_a")
        assert gen.next_sk("dim_b") == 1

    def test_persists_across_instances(self, tmp_path):
        gen1 = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen1.next_sk("dim_x")
        gen1.next_sk("dim_x")
        gen2 = SurrogateKeyGenerator(meta_dir=tmp_path)
        assert gen2.next_sk("dim_x") == 3


class TestBatchNextSk:
    def test_returns_sequential_list(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        result = gen.batch_next_sk("dim_x", 5)
        assert result == [1, 2, 3, 4, 5]

    def test_continues_from_previous(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.batch_next_sk("dim_x", 3)
        result = gen.batch_next_sk("dim_x", 2)
        assert result == [4, 5]

    def test_zero_returns_empty(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        assert gen.batch_next_sk("dim_x", 0) == []

    def test_negative_returns_empty(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        assert gen.batch_next_sk("dim_x", -1) == []


class TestReserveUnknown:
    def test_returns_minus_one(self):
        assert SurrogateKeyGenerator.reserve_unknown() == -1


class TestCurrentMax:
    def test_zero_before_any_sk(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        assert gen.current_max("dim_x") == 0

    def test_reflects_issued_sks(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.batch_next_sk("dim_x", 7)
        assert gen.current_max("dim_x") == 7


class TestReset:
    def test_reset_clears_sequence(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.batch_next_sk("dim_x", 10)
        gen.reset("dim_x")
        assert gen.next_sk("dim_x") == 1

    def test_reset_does_not_affect_other_dims(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.next_sk("dim_a")
        gen.next_sk("dim_b")
        gen.reset("dim_a")
        assert gen.next_sk("dim_b") == 2


class TestAtomicWrite:
    def test_no_tmp_file_remains_after_write(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.next_sk("dim_x")
        tmp_file = tmp_path / "sk_sequences.tmp"
        assert not tmp_file.exists()

    def test_sequence_file_created(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        gen.next_sk("dim_x")
        assert (tmp_path / "sk_sequences.json").exists()


class TestThreadSafety:
    def test_no_duplicate_sks_under_concurrency(self, tmp_path):
        gen = SurrogateKeyGenerator(meta_dir=tmp_path)
        results = []
        lock = threading.Lock()

        def worker():
            sk = gen.next_sk("dim_x")
            with lock:
                results.append(sk)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 20
        assert len(set(results)) == 20
        assert sorted(results) == list(range(1, 21))
