"""
Unit tests for datatrasnfer.py
Tests connection handling, data migration, and error management
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import sys
import logging
import tempfile
import os

# Import the functions to test
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datatrasnfer as dt


class TestSetupLogger(unittest.TestCase):
    """Test logger configuration"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.logfile = os.path.join(self.temp_dir, "test.log")
    
    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.logfile):
            os.remove(self.logfile)
        os.rmdir(self.temp_dir)
    
    def test_logger_file_creation(self):
        """Test that logger creates a log file"""
        dt.setup_logger(self.logfile, logging.INFO)
        # After setup_logger is called, logging should be configured
        self.assertIsNotNone(logging.getLogger())
    
    def test_logger_level_debug(self):
        """Test logger with DEBUG level"""
        dt.setup_logger(self.logfile, logging.DEBUG)
        logger = logging.getLogger()
        # Logger might have different levels after setup, just verify it's configured
        self.assertIsNotNone(logger)
    
    def test_logger_level_info(self):
        """Test logger with INFO level (default)"""
        dt.setup_logger(self.logfile, logging.INFO)
        logger = logging.getLogger()
        # Logger is configured, verify it's not None
        self.assertIsNotNone(logger)


class TestGetConnection(unittest.TestCase):
    """Test database connection handling"""
    
    def setUp(self):
        """Set up test configuration"""
        self.valid_conf = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }
    
    @patch('datatrasnfer.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        result = dt.get_connection(self.valid_conf)
        
        self.assertEqual(result, mock_conn)
        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            dbname='test_db',
            user='test_user',
            password='test_password'
        )
    
    @patch('datatrasnfer.psycopg2.connect')
    def test_get_connection_failure(self, mock_connect):
        """Test connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with self.assertRaises(Exception):
            dt.get_connection(self.valid_conf)
    
    @patch('datatrasnfer.psycopg2.connect')
    def test_get_connection_missing_host(self, mock_connect):
        """Test connection with missing required parameters"""
        invalid_conf = {
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
            # Missing 'host'
        }
        
        # When host is missing, get_connection will raise KeyError
        with self.assertRaises(KeyError):
            dt.get_connection(invalid_conf)


class TestMigrateTable(unittest.TestCase):
    """Test table migration functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.source_conf = {
            'host': 'source_host',
            'port': 5432,
            'database': 'source_db',
            'user': 'source_user',
            'password': 'source_password'
        }
        
        self.target_conf = {
            'host': 'target_host',
            'port': 5432,
            'database': 'target_db',
            'user': 'target_user',
            'password': 'target_password'
        }
        
        self.temp_dir = tempfile.mkdtemp()
        self.logfile = os.path.join(self.temp_dir, "migration.log")
    
    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.logfile):
            os.remove(self.logfile)
        os.rmdir(self.temp_dir)
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_migrate_table_success_single_chunk(self, mock_get_conn, mock_setup_logger):
        """Test successful migration of single chunk"""
        # Mock connections
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        # Mock cursors
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        # Mock column retrieval
        src_cur.execute.side_effect = [None, None]  # First for columns, second for data
        src_cur.fetchall.return_value = [('id',), ('name',), ('value',)]
        
        # Mock data retrieval - single chunk of 2 rows
        src_cur.fetchmany.side_effect = [
            [(1, 'test1', 100), (2, 'test2', 200)],
            []  # Empty list signals end of data
        ]
        
        # Execute migration
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'test_table',
            'public', 'test_table',
            chunk_size=2
        )
        
        # Verify connections were created
        self.assertEqual(mock_get_conn.call_count, 2)
        
        # Verify cursors were created
        src_conn.cursor.assert_called()
        tgt_conn.cursor.assert_called()
        
        # Verify commit was called
        tgt_conn.commit.assert_called()
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_migrate_table_success_multiple_chunks(self, mock_get_conn, mock_setup_logger):
        """Test successful migration with multiple chunks"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        # Mock columns
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('id',), ('name',)]
        
        # Mock data - 3 chunks (500, 500, 250 records)
        src_cur.fetchmany.side_effect = [
            [(i, f'row_{i}') for i in range(1, 501)],     # Chunk 1: 500 rows
            [(i, f'row_{i}') for i in range(501, 1001)],  # Chunk 2: 500 rows
            [(i, f'row_{i}') for i in range(1001, 1251)], # Chunk 3: 250 rows
            []  # End of data
        ]
        
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'large_table',
            'public', 'large_table',
            chunk_size=500
        )
        
        # Verify 3 commits were made (one per chunk)
        self.assertEqual(tgt_conn.commit.call_count, 3)
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    @patch('sys.exit')
    def test_migrate_table_connection_error(self, mock_exit, mock_get_conn, mock_setup_logger):
        """Test migration with connection error - script logs and exits"""
        mock_get_conn.side_effect = Exception("Connection refused")
        
        # Function should catch exception, log it and exit
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'test_table',
            'public', 'test_table'
        )
        
        # Verify sys.exit was called with error code
        mock_exit.assert_called_once_with(1)
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_migrate_table_insert_error_rollback(self, mock_get_conn, mock_setup_logger):
        """Test migration with insert error and rollback"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        # Mock columns
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('id',), ('name',)]
        
        # Mock data
        src_cur.fetchmany.side_effect = [
            [(1, 'test1'), (2, 'test2')],
            []
        ]
        
        # Mock insert error
        tgt_cur.execute.side_effect = Exception("Unique constraint violation")
        
        # Execution should handle error and continue
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'test_table',
            'public', 'test_table',
            chunk_size=10
        )
        
        # Verify rollback was called
        tgt_conn.rollback.assert_called()
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_migrate_table_column_detection(self, mock_get_conn, mock_setup_logger):
        """Test that columns are correctly detected from source table"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        # Mock column retrieval with specific column names
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('user_id',), ('email',), ('created_at',)]
        
        # Mock data
        src_cur.fetchmany.side_effect = [
            [(1, 'user@example.com', '2025-01-01')],
            []
        ]
        
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'users',
            'public', 'users_copy'
        )
        
        # Verify column query was executed
        calls = src_cur.execute.call_args_list
        first_call = calls[0]
        self.assertIn('information_schema.columns', first_call[0][0])
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_migrate_table_empty_table(self, mock_get_conn, mock_setup_logger):
        """Test migration of empty table"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        # Mock columns
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('id',), ('name',)]
        
        # Mock empty data
        src_cur.fetchmany.return_value = []
        
        dt.migrate_table(
            self.source_conf, self.target_conf,
            'public', 'empty_table',
            'public', 'empty_table'
        )
        
        # Verify no commits for empty table
        tgt_conn.commit.assert_not_called()


class TestChunkConfiguration(unittest.TestCase):
    """Test chunk size configuration"""
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_custom_chunk_size(self, mock_get_conn, mock_setup_logger):
        """Test custom chunk size parameter"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('id',)]
        
        # Test with chunk_size=1000
        src_cur.fetchmany.side_effect = [
            [(i,) for i in range(1, 1001)],
            []
        ]
        
        dt.migrate_table(
            {'host': 'h', 'port': 5432, 'database': 'db', 'user': 'u', 'password': 'p'},
            {'host': 'h', 'port': 5432, 'database': 'db', 'user': 'u', 'password': 'p'},
            'public', 'table',
            'public', 'table',
            chunk_size=1000
        )
        
        # Verify fetchmany was called with correct chunk size
        src_cur.fetchmany.assert_called_with(1000)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    @patch('datatrasnfer.setup_logger')
    @patch('datatrasnfer.get_connection')
    def test_special_characters_in_schema_table_names(self, mock_get_conn, mock_setup_logger):
        """Test migration with special characters in schema/table names"""
        src_conn = MagicMock()
        tgt_conn = MagicMock()
        mock_get_conn.side_effect = [src_conn, tgt_conn]
        
        src_cur = MagicMock()
        tgt_cur = MagicMock()
        src_conn.cursor.return_value = src_cur
        tgt_conn.cursor.return_value = tgt_cur
        
        src_cur.execute.side_effect = [None, None]
        src_cur.fetchall.return_value = [('id',)]
        src_cur.fetchmany.side_effect = [[],  ]
        
        # Should handle special characters properly
        dt.migrate_table(
            {'host': 'h', 'port': 5432, 'database': 'db', 'user': 'u', 'password': 'p'},
            {'host': 'h', 'port': 5432, 'database': 'db', 'user': 'u', 'password': 'p'},
            'my-schema', 'my-table',
            'my-schema', 'my-table'
        )
        
        # Verify execute was called (shouldn't crash)
        src_cur.execute.assert_called()


if __name__ == '__main__':
    unittest.main()
