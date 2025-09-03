import os
import sys
from unittest.mock import MagicMock, patch

from src.html_scrap import can_scrape, scrape_helmet_laws

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


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
    import os

    os.environ["HELMET_LAWS_URL"] = (
        "https://www.iihs.org/research-areas/motorcycles/motorcycle-helmet-laws-table"
    )
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = (
        "<html><table><tr><th>State</th><th>Law</th></tr>"
        "<tr><td>Alabama</td><td>Universal</td></tr></table></html>"
    )
    mock_get.return_value = mock_resp

    mock_soup = MagicMock()
    mock_table = MagicMock()

    # Mock header row and data row
    header_row = MagicMock()
    header_cells = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
    header_values = [
        "State",
        "Required to wear helmet",
        "Motorcycle-type vehicles not covered",
        "Footnotes",
    ]
    for cell, value in zip(header_cells, header_values):
        cell.get_text.return_value = value
    header_row.find_all.return_value = header_cells

    data_row = MagicMock()
    data_cells = [MagicMock(), MagicMock()]
    data_values = ["Universal", "None"]
    for cell, value in zip(data_cells, data_values):
        cell.get_text.return_value = value
    data_row.find_all.return_value = data_cells

    mock_table.find_all.side_effect = lambda tag: (
        [header_row, data_row] if tag == "tr" else []
    )
    mock_soup.find.return_value = mock_table
    mock_bs.return_value = mock_soup

    headers, helmet_laws = scrape_helmet_laws()
    assert headers == [
        "State",
        "Required to wear helmet",
        "Motorcycle-type vehicles not covered",
        "Footnotes",
    ]
    assert helmet_laws == [
        {
            "State": "Alabama",
            "Required to wear helmet": "Universal",
            "Motorcycle-type vehicles not covered": "None",
            "Footnotes": "",
        }
    ]
