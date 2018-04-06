import setuptools

setuptools.setup(
    name="deep_learning_101",
    version="0.1.0",
    url="https://github.com/daidaifan/deep-learning-101",

    author="daidaifan",
    author_email="",

    description="Implement deep learning models from scratch",
    long_description=open('README.md').read(),

    packages=setuptools.find_packages(),
    setup_requires=['numpy'],
    install_requires=['numpy',
                      'scipy'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

)
