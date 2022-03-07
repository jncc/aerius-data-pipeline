reinstall:
	pip uninstall -y aeriusdatapipeline
	python setup.py bdist_wheel
	pip install dist\aeriusdatapipeline-0.1-py3-none-any.whl