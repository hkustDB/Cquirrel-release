import io
import json
import os
import unittest

from cquirrel_app import create_app


class TestFileTransfer(unittest.TestCase):
    def setUp(self):
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.client = self.app.test_client()

        # mock a json file
        self.json_content1 = {"swimming": "cool", "running": "hot"}
        self.json_str1 = json.dumps(self.json_content1, indent=4)
        self.json_file_path1 = "./json1.json"
        with open(self.json_file_path1, "w") as f:
            f.write(self.json_str1)

    def tearDown(self):
        if os.path.exists(self.json_file_path1):
            os.remove(self.json_file_path1)
        self.app_context.pop()

    def test_upload(self):
        response = self.client.post(
            '/',
            data={
                'json_file': (io.BytesIO(self.json_str1.encode()), "json1.json")
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )

        # if the response include CodeGen, which means success
        self.assertIn(b"The json file uploaded successfully", response.data)

    def test_upload_empty(self):
        response = self.client.post(
            '/',
            data={
                'json_file': (io.BytesIO(''.encode()), '')
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )
        # if the response include CodeGen, which means success
        self.assertIn(b"The uploaded json file is empty", response.data)

    def test_upload_not_json(self):
        response = self.client.post(
            '/',
            data={
                'json_file': (io.BytesIO('abc'.encode()), 'abc.txt')
            },
            content_type='multipart/form-data',
            follow_redirects=True
        )
        # if the response include CodeGen, which means success
        self.assertIn(b"The uploaded file is not a json file", response.data)

    def test_download_log(self):
        response = self.client.get('/download_log')
        self.assertEqual(response.status_code, 200)

    def test_download_generated_jar(self):
        response = self.client.get('/download_generated_jar')
        self.assertEqual(response.status_code, 200)


if __name__ == '__main__':
    unittest.main()
