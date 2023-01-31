#!/usr/bin/env python3

import os
import sys
import csv
import re

ohdr = ["Polygon",
        "Name",
        "Street Address",
        "Street Address2",
        "City",
        "State",
        "Zip code",
        "Roof Area (Sq. Ft)",
        "Occupancy Type",
        "Construction Type",
        "Roof Type",
        "Roof Construction",
        "Roof Material",
        "Normal Population",
        "Sprinklered",
        "Stand Pipe",
        "Fire Alarm",
        "Hours of Operation",
        "Owner Contact",
        "Owner Phone",
        "Notes",
        "Storey Above",
        "Storey Below"]

ihdr = ["ADDRESS",
        "CITYNAME",
        "COMPANY",
        "Occupancy Type",
        "HOURS_OF_O",
        "NOEMP",
        "WKND_HOLHR",
        "NOEMPWKND",
        "LENGTH",
        "WIDTH",
        "HEIGHT",
        "FLOORS",
        "BASEMENT",
        "Building Construction",
        "ROOFCONST",
        "Sprinklered?",
        "Type of Sprinkler",
        "FIRE_PUMP",
        "NUM_OF_RIS",
        "RISER_LOC_",
        "FDC",
        "FDC LOCATION",
        "Standpipes",
        "WATER_SUPP",
        "GAS_METER Location",
        "ELECTRIC_Meter Location",
        "STAGING",
        "Emergency Vent Systems",
        "Exposures",
        "Exposure Distance",
        "Knox Box Location",
        "Panel Location",
        "Fire PUMPGPM",
        "ALARM#",
        "DATE",
        "Special Considerations",
        "SPECIAL HAZARDS",
        "# of Hydrants",
        "Hydrant LOCATION"]

mapping = {
    "Street Address": "ADDRESS",
    "City":    "CITYNAME",
    "Name":    "COMPANY",
    "Occupancy Type":    "Occupancy Type",
    "Hours of Operation":    "HOURS_OF_O",
    "Normal Population":    "NOEMP",
    "Storey Above":    "FLOORS",
    "Storey Below":    "BASEMENT",
    "Construction Type":    "Building Construction",
    "Roof Construction":    "ROOFCONST",
    "Sprinklered":    "Sprinklered?",
    "Stand Pipe":    "Standpipes",
}

def mandatory(f):
    return f != ""


def numeric(f):
    return f == "" or re.match(r"\d+", f)


def occupancy_type(f):
    legal_values = ["",
                    "assembly",
                    "board & care",
                    "business / mercantile",
                    "day-care",
                    "detention & correctional",
                    "educational",
                    "high hazard",
                    "industrial",
                    "medical care / institutional",
                    "multi-family",
                    "residential",
                    "special structures",
                    "storage"]

    return f.lower() in legal_values


def construction_type(f):
    legal_values = ["",
                    "not classified",
                    "type ia - fire resistive",
                    "type ib - fire resistive non-combustible",
                    "type iia - protective non-combustible",
                    "type iib - unprotected non-combustible",
                    "type iiia - protected ordinary",
                    "type iiib - unprotected ordinary",
                    "type iv - heavy timber",
                    "type va - protected combustible",
                    "type vb - unprotected combustible"]

    return f.lower() in legal_values

def roof_type(f):
    legal_values = ["",
                    "bonnet",
                    "bowstring truss",
                    "butterfly",
                    "combination",
                    "curved",
                    "dome",
                    "flat",
                    "gable",
                    "gambrel",
                    "hip",
                    "jerkin head",
                    "mansard",
                    "pyramid",
                    "saltbox",
                    "sawtooth",
                    "skillion"]

    return f.lower() in legal_values


def roof_construction(f):
    legal_values = ["",
                    "beam - concrete",
                    "beam - steel",
                    "beam - wood",
                    "steel truss - open web",
                    "wood / steel - closed web",
                    "wood / steel - open web",
                    "wood truss - closed web",
                    "wood truss - open web"]

    return f.lower() in legal_values


def roof_material(f):
    legal_values = ["",
                    "built-up",
                    "composition shingles",
                    "membrane - plastic elastomer",
                    "membrane - synthetic elastomer",
                    "metal",
                    "metal - corrugated",
                    "metal - shake",
                    "metal - standing seam",
                    "roof covering not class",
                    "roof covering undetermined/not reported",
                    "shingle - asphalt / composition",
                    "slate - composition",
                    "slate - natural",
                    "structure without roof",
                    "tile - clay",
                    "tile - concrete",
                    "tile (clay, cement, slate, etc.)",
                    "wood - shingle/shake",
                    "wood shakes/shingles (treated)",
                    "wood shakes/shingles (untreated)"]

    return f.lower() in legal_values


def normal_population(f):
    legal_values = ["",
                    "vacant",
                    "1 - 10",
                    "11 - 50",
                    "51 - 100",
                    "101 - 500",
                    "501 - 1000"]

    return f.lower() in legal_values


def sprinklered(f):
    legal_values = ["",
                    "dry system",
                    "wet system",
                    "both",
                    "none"]

    return f.lower() in legal_values

    
def yes_no(f):
    legal_values = ["", "yes", "no"]

    return f.lower() in legal_values


edits = {
    "Street Address": mandatory,
    "City": mandatory,
    "State": mandatory,
    "Roof Area (Sq. Ft)": numeric,
    "Occupancy Type": occupancy_type,
    "Construction Type": construction_type,
    "Roof Type": roof_type,
    "Roof Construction": roof_construction,
    "Roof Material": roof_material,
    "Normal Population": normal_population,
    "Sprinklered": sprinklered,
    "Stand Pipe": yes_no,
    "Fire Alarm": yes_no
}

def get_field(irec, ihdr, fld):
    i = ihdr.index(fld)
    return irec[i]


def refmt(irec, ihdr, ohdr):
    orec = [""] * len(ohdr)
    failed_edits = 0

    for i, o in enumerate(ohdr):
        if o in mapping:
            orec[i] = get_field(irec, ihdr, mapping[o])
            
        # audit output record
        if ohdr[i] in edits:
            f = edits[ohdr[i]]
            if not f(orec[i]):
                print("%s: <%s> failed" % (ohdr[i], orec[i]),
                      file=sys.stderr)
                failed_edits += 1

    return orec, failed_edits
            

def empty_record(rec):
    return sum([len(f) for f in rec]) == 0
        

if __name__ == "__main__":
    r = csv.reader(sys.stdin, delimiter="\t")
    w = csv.writer(sys.stdout, lineterminator="\n")

    hdr = next(r)

    w.writerow(ohdr)

    for recnum, rec in enumerate(r):
        if empty_record(rec):
            continue

        orec, rc = refmt(rec, hdr, ohdr)

        if rc > 0:
            print(recnum, rec, file=sys.stderr)

        w.writerow(orec)
