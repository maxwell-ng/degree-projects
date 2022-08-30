import re

def loadLetterValues(valuesFilePath = "values.txt"):
    """
    Read in the values.txt file with letter scores and return it. 

    returns: dictionary {character: value}
    """
    with open(valuesFilePath) as valuesFile:
        return {line.split()[0] : int(line.split()[1]) for line in valuesFile}
        # probably would've been faster to do a standard loop to avoid two splits but this is shorter
    # else:
    #     print("Could not open values file.")
    #     exit(1) 

def isLastLetter(name, i):
    """
    Check if the character at position i in name is the last letter of a word, where name is in CamelCase. 
    In this case, it means it is followed by an upper case character (indicating a next word), 
    or is the last character of the whole name.

    returns: true if name[i] is last letter of a word as described above
    """
    return i == len(name) -1 or name[i+1].isupper()

def calcScoresInWord(nameCamelCase, letterValues):
    """
    Calculate values for each character in a name.
    Name is expected to be in CamelCase.

    returns: a list of scores where list[i] is the score for name[i]
    """
    name = nameCamelCase # making the variable name shorter to look nicer
    charWordPos = 0 # tracker of a letter's position in a word (first to last)
    scores = [-1] * len(name) # scores array with same indices as input word
    for i in range(len(name)):
        if charWordPos == 0:
            #first letter of a word
            scores[i] = 0
        elif isLastLetter(name, i):
            #last letter of a word (or of the whole name)
            scores[i] = 5 if name[i].upper() != 'E' else 20
        else:
            #middle letter
            bonus = charWordPos if charWordPos <3 else 3 # 1 for 2nd letter, 2 for 3rd, 3 for 4th+
            scores[i] = letterValues[name[i].upper()] + bonus
        # increment charWordPos, or reset to 0 if last letter of word
        charWordPos = 0 if isLastLetter(name,i) else charWordPos + 1
    return scores

        
def reformatName(name):
    """ 
    Clean name as requested in assignment brief, and turn into CamelCase to be used later
    
    returns: the reformatted name
    """
    name = re.sub("'+", "", name) # remove apostrophes
    name = re.sub("[^a-zA-Z]+", " ", name) # replace non-letter chars (inc. spaces) with spaces
    name = name.title() # ensure titlecase (begginnings of words capitalised) ("\b(\w)")
    name = re.sub(" ", "", name) # remove spaces
    return name

def createAbvs(name, letterValues):
    """
    Generate all possible abbreviations for one name along with their scores, 
    as specified in the assignment brief.
    If two equal abbreviations can be created for the name, only the lowest scored one is kept. 

    returns: dictionary {abbreviation: score}
    """
    name = reformatName(name)
    # print(name)

    # get score for all letters before getting abbvs (to avoid doing O(n^2) score calcs)
    scores = calcScoresInWord(name, letterValues)

    abvs = {}
    for i in range(1, len(name) - 1): # need i and j instead of a char loop for getting scores
        for j in range(i+1, len(name)):
            abv = f"{name[0]}{name[i]}{name[j]}".upper()
            score = 0 + scores[i] + scores[j]

            # add to list (dict in this case)
            if abv not in abvs or score < abvs[abv]:
                # only add if not already in, or if already in but new score is lower
                abvs[abv] = score
    return abvs

def createAllAbvs(fpath, letterValues):
    """
    Read a file at fpath and create abbreviations for each line in it (by calling createAbvs()).

    returns: list of pairs of string and dictionary: [(name, {abbreviation: score})]
    """
    with open(fpath) as infile:
        return [(line.rstrip("\n"), createAbvs(line.rstrip("\n"), letterValues)) for line in infile]

def findDuplicates(allabvs):
    """
    Creates a list of abbreviations that occur twice across any names
    """
    #allabvs looks like: [(name: {abv: score})]

    # count number of instances of each abv
    instances = {}
    for name, abvs in allabvs: # for each {abv: score} dict
        for abv in abvs.keys(): # for each abv
            instances[abv] = 1 if abv not in instances else instances[abv] + 1
            
    dupes = {abv for abv in instances.keys() if instances[abv] > 1} # I'm surprised this works tbh
    # print(dupes)
    return dupes
        
def removeDuplicates(allabvs, dupes):
    """
    Create a new allabvs-like list but with any abv appearing in dupes removed.
    To be used in conjunction with createAllAbvs() and findDuplicates()

    returns: [(name, {abbreviation: score})] from allabvs but with select abbvs removed.
    """
    outabvs = []
    for name, abvs in allabvs:
        outabvs.append((name, {abv: score for abv, score in abvs.items() if abv not in dupes}))
    return outabvs


def findAndRemoveDuplicates(allabvs):
    """
    Removes any abbreviations that are not unique by calling findDuplicates() and removeDuplicates().
    If an abbreviation occurs more than once, all its instances are removed and not just the surplus ones.
    
    returns: allabvs but with duplicate abbvs removed
    """
    dupes = findDuplicates(allabvs)
    return removeDuplicates(allabvs, dupes)

def chooseBestAbvsInner(abvs):
    """
    Find the abbreviation with the lowest score, for one name's set of abbreviations.
    If multiple abbvs have the same score, returns all (or none if no abbvs provided).
    Helper function for chooseBestAbvs().

    returns: list of abbreviations (usually of length 1, but sometimes 0 or 2+)
    """

    # ideally minv would have been the first element, but a dictionary is unordered so this could have been risky.
    # for general use it would have been more correct to do it differently but doesn't matter here.
    minv = 1000000 # there is no no max_int in python but scores cannot be higher than 76 anyway
    out = []
    for abv, score in abvs.items():
        if (score < minv):
            # new lower score, replace any old abvs in list
            out = [abv]
            minv = score
        elif (score == minv):
            # two abvs with same lowest score -> add this one
            out.append(abv)
        else:
            # there is already a lower score
            pass
    return out


def chooseBestAbvs(filtered):
    """
    Find the lowest-scored abbreviation for each name.

    returns: list of pairs [(name, [best_abbv, ])]
    """
    return [(name, chooseBestAbvsInner(abvs)) for name, abvs in filtered]

def writeBestAbvsToFile(bestabvs, fpath):
    """
    Writes the output of chooseBestAbvs() into a file in the format specified.
    """
    with open(fpath, 'w') as outfile:
        for name, abvs in bestabvs:
            outfile.write(f"{name}\n") # a line with the name
            outfile.write(f"{(' '.join(abvs))}\n") # a line with the best abbvs for the name


def main():
    """
    Entry point of the program, does what was specified in the brief by calling the other functions.
    """
    fname = input("Please enter the name of your file: ")
    # if fname[-4:] == ".txt":
    #     fname = fname[:-4]  # not doing this in case a file like "my.txt.txt" is to be used.
    # print(fname)
    letterValues = loadLetterValues()
    # print(letterValues)
    allabvs= createAllAbvs(f"{fname}.txt", letterValues)
    # print(allabvs)
    filtered = findAndRemoveDuplicates(allabvs)
    # print(filtered)
    bestabvs = chooseBestAbvs(filtered)
    # print(bestabvs)
    writeBestAbvsToFile(bestabvs, f"pavlica_{fname}_abbrevs.txt")
    

# to enable running this file either by itself or as a module
if __name__ == "__main__":
    main()
