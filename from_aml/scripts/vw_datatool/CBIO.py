from CBSample import Sample

def samples_to_file(samples, file, format = 'dsjson', append = False):
    """
    Write an array of cb impressions to a file.
    
    Arguments:
        samples {array} -- array of CBSample.Sample instances
        file {str} -- name of file to write to
    
    Keyword Arguments:
        format {str} -- CB format to use ('vw' or 'json') (default: {'json'})
        append {bool} -- False to overwrite, True to append (default: {False})
    """
    if format == 'dsjson' or format == 'json':
        _samples_to_dsjson(samples, file, append)
    elif format == 'vw':
        _samples_to_vw(samples, file, append)
    else:
        raise ValueError("Unsupported file format.")

def samples_from_file(file, format = 'dsjson'):
    """
    Read cb impressions from a file into an array of CBSample.Sample.
    
    Arguments:
        file {str} -- name of file to write to
    
    Keyword Arguments:
        format {str} -- CB format to use ('vw' or 'json') (default: {'json'})
    """
    if format == 'dsjson' or format == 'json':
        samples = _samples_from_dsjson(file)
    elif format == 'vw':
        samples = _samples_from_vw(file)
    else:
        raise ValueError("Unsupported file format.")
    return samples

def _samples_to_dsjson(samples, file, append = False):
    mode = 'w' if append == False else 'a'
    with open(file, mode) as f:
        for sample in samples:
            f.write(sample.to_dsjson())

def _samples_to_vw(samples, file, append = False):
    mode = 'w' if append == False else 'a'
    with open(file, mode) as f:
        for sample in samples:
            f.write(sample.to_vw())

def _samples_from_dsjson(file):
    samples = []
    with open(file, 'r') as f:
        for line in f:
            if line.strip():
                samples.append(Sample.from_dsjson(line))
    return samples

def _samples_from_vw(file):
    #TODO: stream line by line
    samples = []
    with open(file, 'r') as f:
        data = f.read().strip()
        if data.startswith('shared'):
            #multi-line vw
            sample_strs = data.split('\n\n')
        else:
            #single-line vw
            sample_strs = data.split('\n')
        for sample_str in sample_strs:
            sample_str = sample_str.strip()
            if sample_str:
                samples.append(Sample.from_vw(sample_str))
    return samples