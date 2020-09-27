from setuptools import setup

install_requirements = [
        'numpy==1.19.2',
        'pandas==1.1.2',
        'requests==2.24.0'
]
setup(
        name='c3api',
        install_requires=install_requirements,
        version='1.0.0',
        packages=['c3api'],
        
)
