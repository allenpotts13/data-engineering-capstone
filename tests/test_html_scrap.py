from unittest.mock import MagicMock, patch

import pytest

from src.html_scrap import can_scrape, scrape_helmet_laws


@patch("src.html_scrap.requests.get")
def test_can_scrape_allows(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "User-agent: *\nAllow: /"
    mock_get.return_value = mock_resp
    assert (
        can_scrape(
            "https://www.iihs.org/research-areas/motorcycles/motorcycle-helmet-laws-table"
        )
        is True
    )


@patch("src.html_scrap.requests.get")
def test_can_scrape_disallows(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "User-agent: *\nDisallow: /research-areas/motorcycles/motorcycle-helmet-laws-table"
    mock_get.return_value = mock_resp
    assert (
        can_scrape(
            "https://www.iihs.org/research-areas/motorcycles/motorcycle-helmet-laws-table"
        )
        is False
    )


@patch("src.html_scrap.requests.get")
@patch("src.html_scrap.BeautifulSoup")
def test_scrape_helmet_laws(mock_bs, mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "<html><table><tr><th>State</th><th>Law</th></tr><tr><td>Alabama</td><td>Universal</td></tr></table></html>"
    mock_get.return_value = mock_resp

    mock_soup = MagicMock()
    mock_table = MagicMock()
    mock_table.find_all.return_value = [
        MagicMock(text="State"),
        MagicMock(text="Law"),
        MagicMock(text="Alabama"),
        MagicMock(text="Universal"),
    ]
    mock_soup.find.return_value = mock_table
    mock_bs.return_value = mock_soup

    headers, helmet_laws = scrape_helmet_laws()
    assert headers == ["State", "Law"]
    assert helmet_laws == [["Alabama", "Universal"]]
