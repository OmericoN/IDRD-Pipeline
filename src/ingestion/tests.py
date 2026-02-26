import unittest
from pathlib import Path
import tempfile
import shutil
import json
from unittest.mock import Mock, patch, MagicMock
import requests

from downloader import PDFDownloader
from converter import GrobidConverter

"""
PDFDownloader tests:

✅ Initialization
✅ Filename generation with sanitization
✅ PDF validation (magic bytes)
✅ Successful download
✅ 404 error handling
✅ 403 Forbidden handling
✅ Missing URL handling
✅ Skip existing files
✅ Timeout handling
✅ Batch download from list
✅ Statistics calculation
GrobidConverter tests:

✅ Initialization
✅ Docker unavailable error
✅ Successful conversion
✅ Skip existing conversions
✅ 503 service unavailable
✅ Timeout handling
✅ Non-existent PDF
✅ Batch conversion
✅ Statistics calculation
✅ Wait for GROBID health check
✅ Context manager usage
Integration tests:

✅ Complete workflow (download → convert)"""

class TestPDFDownloader(unittest.TestCase):
    """Test cases for PDFDownloader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for test outputs
        self.test_dir = tempfile.mkdtemp()
        self.downloader = PDFDownloader(output_dir=self.test_dir)
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
    
    def test_initialization(self):
        """Test downloader initialization."""
        self.assertTrue(Path(self.test_dir).exists())
        self.assertEqual(self.downloader.output_dir, Path(self.test_dir))
        self.assertEqual(self.downloader.max_retries, 3)
        self.assertIn('successful', self.downloader.stats)
    
    def test_generate_filename(self):
        """Test filename generation."""
        # Normal paper ID
        filename = self.downloader.generate_filename("abc123")
        self.assertEqual(filename, "abc123.pdf")
        
        # Paper ID with invalid characters
        filename = self.downloader.generate_filename("abc/123:456")
        self.assertEqual(filename, "abc123456.pdf")
    
    def test_is_valid_pdf(self):
        """Test PDF validation."""
        # Create a valid PDF mock
        valid_pdf = Path(self.test_dir) / "valid.pdf"
        with open(valid_pdf, 'wb') as f:
            f.write(b'%PDF-1.4\n')
        
        self.assertTrue(self.downloader.is_valid_pdf(valid_pdf))
        
        # Create an invalid PDF
        invalid_pdf = Path(self.test_dir) / "invalid.pdf"
        with open(invalid_pdf, 'wb') as f:
            f.write(b'NOT A PDF')
        
        self.assertFalse(self.downloader.is_valid_pdf(invalid_pdf))
        
        # Non-existent file
        self.assertFalse(self.downloader.is_valid_pdf(Path(self.test_dir) / "nonexistent.pdf"))
    
    @patch('downloader.requests.get')
    def test_download_paper_success(self, mock_get):
        """Test successful PDF download."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Type': 'application/pdf', 'content-length': '1000'}
        mock_response.content = b'%PDF-1.4\nMock PDF content'
        mock_response.iter_content = lambda chunk_size: [mock_response.content]
        mock_get.return_value = mock_response
        
        success, message = self.downloader.download_paper(
            paper_id="test123",
            url="https://example.com/paper.pdf"
        )
        
        self.assertTrue(success)
        self.assertIn("Downloaded", message)
        self.assertEqual(self.downloader.stats['successful'], 1)
        
        # Check file was created
        output_file = Path(self.test_dir) / "test123.pdf"
        self.assertTrue(output_file.exists())
    
    @patch('downloader.requests.get')
    def test_download_paper_404(self, mock_get):
        """Test download with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        success, message = self.downloader.download_paper(
            paper_id="test123",
            url="https://example.com/notfound.pdf"
        )
        
        self.assertFalse(success)
        self.assertIn("404", message)
        self.assertEqual(self.downloader.stats['failed'], 1)
    
    @patch('downloader.requests.get')
    def test_download_paper_403(self, mock_get):
        """Test download with 403 Forbidden error."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response
        
        success, message = self.downloader.download_paper(
            paper_id="test123",
            url="https://example.com/forbidden.pdf"
        )
        
        self.assertFalse(success)
        self.assertIn("403", message)
        self.assertEqual(self.downloader.stats['failed'], 1)
    
    def test_download_paper_no_url(self):
        """Test download with no URL provided."""
        success, message = self.downloader.download_paper(
            paper_id="test123",
            url=""
        )
        
        self.assertFalse(success)
        self.assertIn("No URL", message)
        self.assertEqual(self.downloader.stats['skipped'], 1)
    
    @patch('downloader.requests.get')
    def test_download_paper_already_exists(self, mock_get):
        """Test skipping download if file already exists."""
        # Create existing valid PDF
        existing_file = Path(self.test_dir) / "existing123.pdf"
        with open(existing_file, 'wb') as f:
            f.write(b'%PDF-1.4\nExisting content')
        
        success, message = self.downloader.download_paper(
            paper_id="existing123",
            url="https://example.com/paper.pdf",
            overwrite=False
        )
        
        self.assertTrue(success)
        self.assertIn("Already exists", message)
        self.assertEqual(self.downloader.stats['skipped'], 1)
        # Should not make HTTP request
        mock_get.assert_not_called()
    
    @patch('downloader.requests.get')
    def test_download_paper_timeout(self, mock_get):
        """Test download timeout handling."""
        mock_get.side_effect = requests.exceptions.Timeout()
        
        success, message = self.downloader.download_paper(
            paper_id="test123",
            url="https://example.com/paper.pdf"
        )
        
        self.assertFalse(success)
        self.assertIn("Timeout", message)
        self.assertEqual(self.downloader.stats['failed'], 1)
    
    @patch('downloader.requests.get')
    def test_download_papers_from_list(self, mock_get):
        """Test batch download from list."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Type': 'application/pdf', 'content-length': '1000'}
        mock_response.content = b'%PDF-1.4\nMock PDF content'
        mock_response.iter_content = lambda chunk_size: [mock_response.content]
        mock_get.return_value = mock_response
        
        papers = [
            {
                'paperId': 'paper1',
                'title': 'Test Paper 1',
                'openAccessPdf': {'url': 'https://example.com/paper1.pdf'}
            },
            {
                'paperId': 'paper2',
                'title': 'Test Paper 2',
                'openAccessPdf': {'url': 'https://example.com/paper2.pdf'}
            }
        ]
        
        results = self.downloader.download_papers_from_list(
            papers,
            delay=0  # No delay for tests
        )
        
        self.assertEqual(len(results['results']), 2)
        self.assertEqual(results['stats']['successful'], 2)
        
        # Check files were created
        self.assertTrue((Path(self.test_dir) / "paper1.pdf").exists())
        self.assertTrue((Path(self.test_dir) / "paper2.pdf").exists())
    
    def test_get_statistics(self):
        """Test statistics calculation."""
        self.downloader.stats = {
            'successful': 8,
            'failed': 2,
            'skipped': 5,
            'total_size': 10485760  # 10 MB
        }
        
        stats = self.downloader.get_statistics()
        
        self.assertEqual(stats['successful'], 8)
        self.assertEqual(stats['failed'], 2)
        self.assertEqual(stats['success_rate'], 80.0)
        self.assertAlmostEqual(stats['total_size_mb'], 10.0, places=1)
        self.assertAlmostEqual(stats['avg_size_mb'], 1.25, places=2)


class TestGrobidConverter(unittest.TestCase):
    """Test cases for GrobidConverter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.test_pdf_dir = tempfile.mkdtemp()
        self.test_output_dir = tempfile.mkdtemp()
        
        # Create mock PDF
        self.test_pdf = Path(self.test_pdf_dir) / "test_paper.pdf"
        with open(self.test_pdf, 'wb') as f:
            f.write(b'%PDF-1.4\nMock PDF content')
    
    def tearDown(self):
        """Clean up after tests."""
        if Path(self.test_pdf_dir).exists():
            shutil.rmtree(self.test_pdf_dir)
        if Path(self.test_output_dir).exists():
            shutil.rmtree(self.test_output_dir)
    
    @patch('converter.docker.from_env')
    def test_initialization(self, mock_docker):
        """Test converter initialization."""
        mock_docker.return_value = Mock()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        self.assertEqual(converter.pdf_dir, Path(self.test_pdf_dir))
        self.assertEqual(converter.output_dir, Path(self.test_output_dir))
        self.assertTrue(converter.output_dir.exists())
        self.assertEqual(converter.grobid_port, 8070)
    
    @patch('converter.docker.from_env')
    def test_initialization_docker_not_available(self, mock_docker):
        """Test initialization when Docker is not available."""
        mock_docker.side_effect = Exception("Docker not running")
        
        with self.assertRaises(RuntimeError) as context:
            GrobidConverter(
                pdf_dir=self.test_pdf_dir,
                output_dir=self.test_output_dir
            )
        
        self.assertIn("Docker not available", str(context.exception))
    
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_convert_pdf_success(self, mock_docker, mock_post):
        """Test successful PDF conversion."""
        # Mock Docker client
        mock_docker.return_value = Mock()
        
        # Mock successful GROBID response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<?xml version="1.0" encoding="UTF-8"?><TEI>Mock TEI XML</TEI>'
        mock_post.return_value = mock_response
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        success, message = converter.convert_pdf(
            pdf_path=self.test_pdf,
            paper_id="test_paper"
        )
        
        self.assertTrue(success)
        self.assertIn("Converted", message)
        self.assertEqual(converter.stats['successful'], 1)
        
        # Check XML file was created
        output_file = Path(self.test_output_dir) / "test_paper.tei.xml"
        self.assertTrue(output_file.exists())
    
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_convert_pdf_already_exists(self, mock_docker, mock_post):
        """Test skipping conversion if XML already exists."""
        mock_docker.return_value = Mock()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        # Create existing XML file
        existing_xml = Path(self.test_output_dir) / "existing.tei.xml"
        with open(existing_xml, 'w') as f:
            f.write('<?xml version="1.0"?><TEI>Existing</TEI>')
        
        success, message = converter.convert_pdf(
            pdf_path=self.test_pdf,
            paper_id="existing",
            overwrite=False
        )
        
        self.assertTrue(success)
        self.assertIn("Already converted", message)
        self.assertEqual(converter.stats['skipped'], 1)
        # Should not make HTTP request
        mock_post.assert_not_called()
    
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_convert_pdf_service_unavailable(self, mock_docker, mock_post):
        """Test conversion with 503 service unavailable."""
        mock_docker.return_value = Mock()
        
        mock_response = Mock()
        mock_response.status_code = 503
        mock_post.return_value = mock_response
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        success, message = converter.convert_pdf(
            pdf_path=self.test_pdf,
            paper_id="test_paper"
        )
        
        self.assertFalse(success)
        self.assertIn("503", message)
        self.assertEqual(converter.stats['failed'], 1)
    
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_convert_pdf_timeout(self, mock_docker, mock_post):
        """Test conversion timeout handling."""
        mock_docker.return_value = Mock()
        mock_post.side_effect = requests.exceptions.Timeout()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        success, message = converter.convert_pdf(
            pdf_path=self.test_pdf,
            paper_id="test_paper"
        )
        
        self.assertFalse(success)
        self.assertIn("timeout", message.lower())
        self.assertEqual(converter.stats['failed'], 1)
    
    @patch('converter.docker.from_env')
    def test_convert_pdf_not_found(self, mock_docker):
        """Test conversion with non-existent PDF."""
        mock_docker.return_value = Mock()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        nonexistent_pdf = Path(self.test_pdf_dir) / "nonexistent.pdf"
        
        success, message = converter.convert_pdf(
            pdf_path=nonexistent_pdf,
            paper_id="nonexistent"
        )
        
        self.assertFalse(success)
        self.assertIn("not found", message.lower())
        self.assertEqual(converter.stats['failed'], 1)
    
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_convert_pdfs_batch(self, mock_docker, mock_post):
        """Test batch PDF conversion."""
        mock_docker.return_value = Mock()
        
        # Create multiple test PDFs
        pdf1 = Path(self.test_pdf_dir) / "paper1.pdf"
        pdf2 = Path(self.test_pdf_dir) / "paper2.pdf"
        
        for pdf in [pdf1, pdf2]:
            with open(pdf, 'wb') as f:
                f.write(b'%PDF-1.4\nMock PDF')
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<?xml version="1.0"?><TEI>Mock TEI</TEI>'
        mock_post.return_value = mock_response
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        results = converter.convert_pdfs(delay=0)
        
        self.assertEqual(len(results['results']), 3)  # Including test_paper.pdf from setUp
        self.assertEqual(results['stats']['successful'], 3)
        
        # Check XML files were created
        self.assertTrue((Path(self.test_output_dir) / "paper1.tei.xml").exists())
        self.assertTrue((Path(self.test_output_dir) / "paper2.tei.xml").exists())
    
    @patch('converter.docker.from_env')
    def test_get_statistics(self, mock_docker):
        """Test statistics calculation."""
        mock_docker.return_value = Mock()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        converter.stats = {
            'successful': 7,
            'failed': 3,
            'skipped': 2
        }
        
        stats = converter.get_statistics()
        
        self.assertEqual(stats['successful'], 7)
        self.assertEqual(stats['failed'], 3)
        self.assertEqual(stats['success_rate'], 70.0)
    
    @patch('converter.requests.get')
    @patch('converter.docker.from_env')
    def test_wait_for_grobid_success(self, mock_docker, mock_get):
        """Test waiting for GROBID to be ready."""
        mock_docker.return_value = Mock()
        
        # Mock successful health check
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        result = converter._wait_for_grobid(timeout=5)
        self.assertTrue(result)
    
    @patch('converter.requests.get')
    @patch('converter.docker.from_env')
    def test_wait_for_grobid_timeout(self, mock_docker, mock_get):
        """Test GROBID wait timeout."""
        mock_docker.return_value = Mock()
        
        # Mock failed health check
        mock_get.side_effect = requests.exceptions.ConnectionError()
        
        converter = GrobidConverter(
            pdf_dir=self.test_pdf_dir,
            output_dir=self.test_output_dir
        )
        
        result = converter._wait_for_grobid(timeout=2)
        self.assertFalse(result)
    
    @patch('converter.docker.from_env')
    def test_context_manager(self, mock_docker):
        """Test context manager usage."""
        mock_client = Mock()
        mock_container = Mock()
        mock_container.status = 'exited'
        
        mock_client.containers.get.side_effect = Exception("Not found")
        mock_client.containers.run.return_value = mock_container
        mock_client.images.get.return_value = Mock()
        
        mock_docker.return_value = mock_client
        
        with patch('converter.GrobidConverter._wait_for_grobid', return_value=True):
            with GrobidConverter(
                pdf_dir=self.test_pdf_dir,
                output_dir=self.test_output_dir
            ) as converter:
                self.assertIsNotNone(converter)
            
            # Verify stop was called
            mock_container.stop.assert_called_once()


class TestIntegration(unittest.TestCase):
    """Integration tests for downloader and converter together."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.pdf_dir = Path(self.test_dir) / "pdfs"
        self.output_dir = Path(self.test_dir) / "converted"
        self.pdf_dir.mkdir()
        self.output_dir.mkdir()
    
    def tearDown(self):
        """Clean up after tests."""
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
    
    @patch('downloader.requests.get')
    @patch('converter.requests.post')
    @patch('converter.docker.from_env')
    def test_download_and_convert_workflow(self, mock_docker, mock_post_grobid, mock_get_download):
        """Test complete workflow: download -> convert."""
        # Mock Docker
        mock_docker.return_value = Mock()
        
        # Mock successful PDF download
        mock_response_download = Mock()
        mock_response_download.status_code = 200
        mock_response_download.headers = {'Content-Type': 'application/pdf', 'content-length': '1000'}
        mock_response_download.content = b'%PDF-1.4\nMock PDF content'
        mock_response_download.iter_content = lambda chunk_size: [mock_response_download.content]
        mock_get_download.return_value = mock_response_download
        
        # Mock successful GROBID conversion
        mock_response_grobid = Mock()
        mock_response_grobid.status_code = 200
        mock_response_grobid.text = '<?xml version="1.0"?><TEI>Mock TEI</TEI>'
        mock_post_grobid.return_value = mock_response_grobid
        
        # Step 1: Download
        downloader = PDFDownloader(output_dir=str(self.pdf_dir))
        success, _ = downloader.download_paper(
            paper_id="test123",
            url="https://example.com/paper.pdf"
        )
        self.assertTrue(success)
        
        # Step 2: Convert
        converter = GrobidConverter(
            pdf_dir=str(self.pdf_dir),
            output_dir=str(self.output_dir)
        )
        success, _ = converter.convert_pdf(
            pdf_path=self.pdf_dir / "test123.pdf",
            paper_id="test123"
        )
        self.assertTrue(success)
        
        # Verify both files exist
        self.assertTrue((self.pdf_dir / "test123.pdf").exists())
        self.assertTrue((self.output_dir / "test123.tei.xml").exists())


def run_tests():
    """Run all tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestPDFDownloader))
    suite.addTests(loader.loadTestsFromTestCase(TestGrobidConverter))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"{'='*60}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)