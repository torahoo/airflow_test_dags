import logging
import os

class Logger:
    def __init__(self, name, log_file=None, level=logging.DEBUG):
        """
        Logger 초기화

        :param name: 로거 이름
        :param log_file: 로그 파일 경로 (None이면 파일에 기록하지 않음)
        :param level: 로그 수준
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 포매터 설정
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # 콘솔 핸들러 추가
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 파일 핸들러 추가 (log_file이 주어진 경우)
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)  # 디렉토리 자동 생성
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        """로거 객체 반환"""
        return self.logger