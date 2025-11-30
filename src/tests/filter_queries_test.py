import pytest
import pandas as pd
from filter_queries import (
    find_exact,
    find_values,
    find_range,
    find_substring
)



class TestFindByExact:
    def test_finds_match(self):
        df = pd.DataFrame({'director': ['George Lucas', 'Steven Spielberg']})
        result = find_exact(df, 'director', 'George Lucas')
        assert len(result) == 1
        assert result.iloc[0]['director'] == 'George Lucas'


class TestFindByValues:
    def test_finds_matches(self):
        df = pd.DataFrame({'director': ['George Lucas', 'Steven Spielberg', 'James Cameron']})
        result = find_values(df, 'director', ['George Lucas', 'James Cameron'])
        assert len(result) == 2


class TestFindByRange:
    def test_finds_matches(self):
        df = pd.DataFrame({'durationMins': [90, 120, 150, 180]})
        result = find_range(df, 'durationMins', 100, 160)
        assert len(result) == 2
    
    def test_datetime_column(self):
        df = pd.DataFrame({
            'release_year': pd.to_datetime(['1980', '1990', '2000', '2010'], format='%Y')
        })
        result = find_range(df, 'release_year', 1985, 2005)
        assert len(result) == 2
    

class TestFindBySubstring:
    def test_finds_matches(self):
        df = pd.DataFrame({'genres': ['Action, Adventure', 'Comedy', 'Action, Sci-Fi']})
        result = find_substring(df, 'genres', 'Action')
        assert len(result) == 2
    
if __name__ == "__main__":
    pytest.main([__file__, "-v"])