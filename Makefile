install:
	python setup.py bdist_wheel
	pip install dist\aeriusdatapipeline-0.1-py3-none-any.whl

reinstall:
	pip uninstall -y aeriusdatapipeline
	python setup.py bdist_wheel
	pip install dist\aeriusdatapipeline-0.1-py3-none-any.whl

clean:
	rd /s /q "dist"
	rd /s /q "build"
	rd /s /q "aeriusdatapipeline.egg-info"

cov:
	pytest --cov aeriusdatapipeline